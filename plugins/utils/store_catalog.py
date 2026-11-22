"""
Steam 스토어 카탈로그·가격 스냅샷 공통 로직

- Kafka 메시지·스냅샷 manifest 형식 정의
- IStoreQueryService/Query 항목을 가격·리뷰 요약 메시지로 정규화
- 전체 스냅샷과 열린 구간(valid_to IS NULL)을 비교해 구간 변경 목록 계산

I/O 없는 순수 함수만 두어 오프라인 테스트로 검증한다.
"""

from __future__ import annotations

from typing import Any, NamedTuple, TypedDict

import polars as pl

# Query 응답 item.type 값 중 게임
STORE_ITEM_TYPE_GAME = 0

# producer가 스냅샷 prefix에 남기는 완료 기록 파일명
MANIFEST_FILENAME = "_manifest.json"


# ── 메시지·manifest 형식 ──────────────────────────────────────


class AppListMessage(TypedDict):
    """steam.app_list 토픽 메시지 (GetAppList 항목 1개)"""

    appid: int
    snapshot_id: str
    collected_at: int
    name: str | None
    last_modified: int | None
    price_change_number: int | None


class StorePriceMessage(TypedDict):
    """
    steam.prices 토픽 메시지 (Query 항목 1개)

    - 가격 단위는 Steam 응답의 cents 문자열을 정수로 변환한 값 그대로
    - 시각 필드는 Unix 초
    """

    appid: int
    snapshot_id: str
    collected_at: int
    name: str | None
    is_free: bool
    has_price: bool
    original_price_cents: int | None
    final_price_cents: int | None
    discount_pct: int
    discount_end_at: int | None
    review_count: int | None
    percent_positive: int | None
    review_score: int | None


class SnapshotManifest(TypedDict):
    """
    producer가 Kafka 전송 확인 후 S3에 남기는 스냅샷 완료 기록

    - Silver는 이 기록의 건수와 실제 적재 건수가 일치할 때만 구간을 갱신
    """

    source: str
    snapshot_id: str
    collected_at: int
    records: int
    api_total: int | None
    skipped: int


class IntervalDiff(NamedTuple):
    """
    스냅샷과 열린 구간의 비교 결과

    :param touch_keys: 값이 같아 last_seen_at만 갱신할 키
    :param close_keys: 값이 바뀌었거나 스냅샷에서 사라져 구간을 닫을 키
    :param insert_rows: 새 구간으로 추가할 스냅샷 행
    """

    touch_keys: list[int]
    close_keys: list[int]
    insert_rows: pl.DataFrame


# ── Query 항목 정규화 ──────────────────────────────────────


def _cents(value: Any) -> int | None:
    """Steam cents 문자열·숫자를 정수로 변환 (값 없음은 None)"""
    if value is None or value == "":
        return None
    return int(value)


def normalize_store_item(
    item: dict[str, Any],
    snapshot_id: str,
    collected_at: int,
) -> StorePriceMessage | None:
    """
    Query 응답 항목을 가격·리뷰 요약 메시지로 변환

    - 게임(type 0)이 아니면 None
    - best_purchase_option이 없으면 has_price=False (무료 또는 KR 구매 불가)
    - 할인 중이 아니면 original_price_in_cents가 없으므로 정가를 최종가로 채움
    - 할인 종료 시각은 active_discounts 중 가장 이른 종료 시각

    :param item: Query 응답의 store_items 항목
    :param snapshot_id: 이번 전체 순회 식별자
    :param collected_at: 순회 시작 Unix 초
    :return: 정규화한 메시지. 게임이 아니면 None
    :raises ValueError: appid 누락
    """
    if item.get("type", STORE_ITEM_TYPE_GAME) != STORE_ITEM_TYPE_GAME:
        return None

    appid = item.get("appid") or item.get("id")
    if not appid:
        raise ValueError(f"Query 항목에 appid 없음: item 키={sorted(item)}")

    option = item.get("best_purchase_option") or {}
    final_price = _cents(option.get("final_price_in_cents"))
    original_price = _cents(option.get("original_price_in_cents"))
    if original_price is None:
        original_price = final_price

    end_dates = [
        int(discount["discount_end_date"])
        for discount in option.get("active_discounts") or []
        if discount.get("discount_end_date")
    ]
    summary = (item.get("reviews") or {}).get("summary_filtered") or {}

    return {
        "appid": int(appid),
        "snapshot_id": snapshot_id,
        "collected_at": collected_at,
        "name": item.get("name"),
        "is_free": bool(item.get("is_free", False)),
        "has_price": bool(option),
        "original_price_cents": original_price,
        "final_price_cents": final_price,
        "discount_pct": int(option.get("discount_pct") or 0),
        "discount_end_at": min(end_dates) if end_dates else None,
        "review_count": summary.get("review_count"),
        "percent_positive": summary.get("percent_positive"),
        "review_score": summary.get("review_score"),
    }


def to_app_list_message(
    app: dict[str, Any],
    snapshot_id: str,
    collected_at: int,
) -> AppListMessage:
    """
    GetAppList 항목을 메시지로 변환

    :raises ValueError: appid 누락
    """
    if not app.get("appid"):
        raise ValueError(f"GetAppList 항목에 appid 없음: item 키={sorted(app)}")

    return {
        "appid": int(app["appid"]),
        "snapshot_id": snapshot_id,
        "collected_at": collected_at,
        "name": app.get("name"),
        "last_modified": app.get("last_modified"),
        "price_change_number": app.get("price_change_number"),
    }


# ── 구간 비교 ──────────────────────────────────────


def diff_snapshot(
    open_intervals: pl.DataFrame,
    snapshot: pl.DataFrame,
    key: str,
    value_columns: list[str],
) -> IntervalDiff:
    """
    전체 스냅샷과 현재 열린 구간을 비교해 구간 변경 목록 계산

    - 양쪽에 있고 값이 모두 같으면 touch
    - 양쪽에 있고 값이 하나라도 다르면 close + insert
    - 스냅샷에만 있으면 insert
    - 열린 구간에만 있으면 close (이번 순회에서 관측되지 않음)
    - NULL끼리는 같은 값으로 비교

    :param open_intervals: 열린 구간 (key + value_columns)
    :param snapshot: 이번 전체 스냅샷 (key + value_columns, key 중복 없음)
    :param key: 비교 키 컬럼명
    :param value_columns: 변경 여부를 판단할 컬럼
    :return: 구간 변경 목록
    :raises ValueError: 스냅샷에 key 중복
    """
    if snapshot.height != snapshot[key].n_unique():
        raise ValueError(
            f"스냅샷 키 중복: key={key}, 행={snapshot.height}, 고유={snapshot[key].n_unique()}"
        )

    # 열린 구간 컬럼을 스냅샷 형에 맞춰 형 차이로 인한 오판 방지
    current = open_intervals.select(
        [pl.col(key).cast(snapshot.schema[key])]
        + [
            pl.col(column).cast(snapshot.schema[column]).alias(f"{column}__open")
            for column in value_columns
        ]
    ).with_columns(pl.lit(True).alias("__has_open"))

    unchanged = pl.all_horizontal(
        [
            pl.col(column).eq_missing(pl.col(f"{column}__open"))
            for column in value_columns
        ]
    )
    flagged = (
        snapshot.with_columns(pl.lit(True).alias("__in_snapshot"))
        .join(current, on=key, how="full", coalesce=True)
        .with_columns(
            pl.col("__in_snapshot").fill_null(False),
            pl.col("__has_open").fill_null(False),
        )
        .with_columns(unchanged.alias("__unchanged"))
    )

    touch = flagged.filter(
        pl.col("__in_snapshot") & pl.col("__has_open") & pl.col("__unchanged")
    )
    changed = flagged.filter(
        pl.col("__in_snapshot") & pl.col("__has_open") & ~pl.col("__unchanged")
    )
    new = flagged.filter(pl.col("__in_snapshot") & ~pl.col("__has_open"))
    missing = flagged.filter(~pl.col("__in_snapshot") & pl.col("__has_open"))

    insert_keys = pl.concat([changed.select(key), new.select(key)])
    return IntervalDiff(
        touch_keys=touch[key].to_list(),
        close_keys=pl.concat([changed[key], missing[key]]).to_list(),
        insert_rows=snapshot.join(insert_keys, on=key, how="semi"),
    )
