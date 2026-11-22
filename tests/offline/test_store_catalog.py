"""
재구성 3단계(게임 목록·가격 정기 수집) 오프라인 검증

- Query 항목 정규화와 스냅샷·열린 구간 비교 (순수 함수)
- producer의 전송 확인 후 manifest 반환, 중복 appid 처리
- Silver 스냅샷 검증: manifest·건수·다른 스냅샷 혼입·급감·순서 역전
- Query 전체 순회 중단 감지와 API key 가림
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest
import requests

import hooks.steam_api as steam_api_module
from hooks.steam_api import SteamApiHook
from operators.silver_store_snapshot_to_db import (
    PRICE_INTERVALS,
    SilverStorePriceIntervalsOperator,
)
from operators.steam_store_snapshot_to_kafka import (
    SnapshotPage,
    SteamStorePricesToKafkaOperator,
    StoreSnapshotToKafkaOperator,
)
from utils.store_catalog import MANIFEST_FILENAME, diff_snapshot, normalize_store_item

SNAPSHOT = "20261122T183000"
COLLECTED_AT = 1795372200


def store_item(appid: int, **overrides: Any) -> dict[str, Any]:
    """정가 판매 중인 최소 Query 항목"""
    item: dict[str, Any] = {
        "appid": appid,
        "type": 0,
        "name": f"game {appid}",
        "is_free": False,
        "best_purchase_option": {"packageid": 1, "final_price_in_cents": "1100000"},
        "reviews": {
            "summary_filtered": {
                "review_count": 10,
                "percent_positive": 90,
                "review_score": 8,
            }
        },
    }
    item.update(overrides)
    return item


# ── Query 항목 정규화 ──────────────────────────────────────


def test_discounted_item_keeps_original_final_and_earliest_end_date() -> None:
    item = store_item(
        1,
        best_purchase_option={
            "final_price_in_cents": "55000",
            "original_price_in_cents": "110000",
            "discount_pct": 50,
            "active_discounts": [
                {"discount_end_date": 1795456800},
                {"discount_end_date": 1795000000},
            ],
        },
    )

    message = normalize_store_item(item, SNAPSHOT, COLLECTED_AT)

    assert message is not None
    assert (message["original_price_cents"], message["final_price_cents"]) == (
        110000,
        55000,
    )
    assert message["discount_pct"] == 50
    assert message["discount_end_at"] == 1795000000, "가장 이른 할인 종료 시각이어야 함"
    assert message["has_price"] is True


def test_full_price_item_fills_original_with_final_price() -> None:
    message = normalize_store_item(store_item(2), SNAPSHOT, COLLECTED_AT)

    assert message is not None
    assert message["original_price_cents"] == message["final_price_cents"] == 1100000
    assert message["discount_pct"] == 0
    assert message["discount_end_at"] is None
    assert message["review_count"] == 10


def test_item_without_purchase_option_is_recorded_without_price() -> None:
    message = normalize_store_item(
        store_item(3, is_free=True, best_purchase_option=None), SNAPSHOT, COLLECTED_AT
    )

    assert message is not None
    assert message["has_price"] is False
    assert message["is_free"] is True
    assert message["final_price_cents"] is None


def test_non_game_item_is_skipped_and_missing_appid_is_rejected() -> None:
    assert normalize_store_item(store_item(4, type=14), SNAPSHOT, COLLECTED_AT) is None

    with pytest.raises(ValueError, match="appid 없음"):
        normalize_store_item({"type": 0, "name": "x"}, SNAPSHOT, COLLECTED_AT)


# ── 스냅샷·열린 구간 비교 ──────────────────────────────────────


def frame(rows: list[tuple[int, int | None, int | None]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={"app_id": pl.Int64, "price": pl.Int64, "end_at": pl.Int64},
        orient="row",
    )


def test_diff_classifies_unchanged_changed_new_and_missing() -> None:
    open_intervals = frame([(1, 100, None), (2, 100, None), (3, 100, None)])
    snapshot = frame([(1, 100, None), (2, 50, 999), (4, 70, None)])

    diff = diff_snapshot(
        open_intervals, snapshot, key="app_id", value_columns=["price", "end_at"]
    )

    assert diff.touch_keys == [1]
    assert sorted(diff.close_keys) == [2, 3], "변경(2)과 미관측(3)은 닫아야 함"
    assert sorted(diff.insert_rows["app_id"].to_list()) == [2, 4], (
        "변경(2)과 신규(4)는 새 구간"
    )


def test_diff_treats_null_as_equal_and_casts_db_types() -> None:
    # DB에서 읽은 열린 구간은 형이 다를 수 있음 (예: Int32)
    open_intervals = frame([(1, None, None)]).with_columns(
        pl.col("price").cast(pl.Int32)
    )
    snapshot = frame([(1, None, None)])

    diff = diff_snapshot(
        open_intervals, snapshot, key="app_id", value_columns=["price", "end_at"]
    )

    assert diff.touch_keys == [1]
    assert diff.close_keys == []
    assert diff.insert_rows.height == 0


def test_diff_with_empty_open_intervals_inserts_everything() -> None:
    empty = frame([])
    snapshot = frame([(1, 100, None), (2, 200, None)])

    diff = diff_snapshot(
        empty, snapshot, key="app_id", value_columns=["price", "end_at"]
    )

    assert diff.touch_keys == []
    assert diff.close_keys == []
    assert diff.insert_rows.height == 2


def test_diff_rejects_duplicate_snapshot_keys() -> None:
    with pytest.raises(ValueError, match="스냅샷 키 중복"):
        diff_snapshot(
            frame([]), frame([(1, 1, None), (1, 2, None)]), "app_id", ["price"]
        )


# ── producer ──────────────────────────────────────


class FakeKafkaHook:
    """send·flush 호출 순서를 기록하는 Kafka Hook 대체"""

    def __init__(self, fail_on_flush: int | None = None) -> None:
        self.events: list[str] = []
        self.sent: list[dict[str, Any]] = []
        self.fail_on_flush = fail_on_flush

    def send(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:  # noqa: ARG002
        self.sent.append(value)
        self.events.append(f"send:{key}")

    def flush(self) -> None:
        flushes = sum(1 for event in self.events if event == "flush")
        if self.fail_on_flush is not None and flushes == self.fail_on_flush:
            raise RuntimeError("delivery failed")
        self.events.append("flush")


class StaticPagesOperator(StoreSnapshotToKafkaOperator):
    """고정 페이지를 순회하는 테스트용 producer"""

    source = "test"

    def __init__(self, pages: list[SnapshotPage], **kwargs: Any) -> None:
        super().__init__(
            kafka_topic="t",
            snapshot_id=SNAPSHOT,
            snapshot_prefix="p",
            task_id="produce",
            **kwargs,
        )
        self.pages = pages

    def iter_snapshot_pages(self, api_hook: Any, collected_at: int):  # noqa: ARG002
        yield from self.pages


def message(appid: int) -> dict[str, Any]:
    return {"appid": appid, "snapshot_id": SNAPSHOT, "collected_at": COLLECTED_AT}


def test_publish_flushes_every_page_and_counts_unique_records() -> None:
    pages = [
        SnapshotPage([message(1), message(2)], skipped=1, api_total=4),
        SnapshotPage([message(2), message(3)], skipped=0, api_total=4),
    ]
    kafka = FakeKafkaHook()

    manifest = StaticPagesOperator(pages=pages).publish(MagicMock(), kafka)

    assert kafka.events == ["send:1", "send:2", "flush", "send:3", "flush"], (
        kafka.events
    )
    assert manifest["records"] == 3
    assert manifest["skipped"] == 2, "게임 아닌 항목 1 + 중복 appid 1"
    assert manifest["api_total"] == 4
    assert manifest["snapshot_id"] == SNAPSHOT


def test_failed_delivery_returns_no_manifest() -> None:
    pages = [SnapshotPage([message(1)], 0, None), SnapshotPage([message(2)], 0, None)]

    with pytest.raises(RuntimeError, match="delivery failed"):
        StaticPagesOperator(pages=pages).publish(
            MagicMock(), FakeKafkaHook(fail_on_flush=1)
        )


def test_store_prices_producer_skips_non_game_items() -> None:
    api = MagicMock()
    api.iter_store_query_pages.return_value = iter(
        [([store_item(1), store_item(2, type=14)], 2)]
    )
    operator = SteamStorePricesToKafkaOperator(
        task_id="prices", kafka_topic="t", snapshot_id=SNAPSHOT, snapshot_prefix="p"
    )

    pages = list(operator.iter_snapshot_pages(api, COLLECTED_AT))

    assert [m["appid"] for m in pages[0].messages] == [1]
    assert pages[0].skipped == 1
    request = api.iter_store_query_pages.call_args.kwargs
    assert request["context"]["country_code"] == "KR"
    assert request["data_request"] == {
        "include_all_purchase_options": True,
        "include_reviews": True,
    }


# ── Silver 스냅샷 검증 ──────────────────────────────────────


class FakeSnapshotS3:
    """manifest와 parquet을 키별로 돌려주는 S3 Hook 대체"""

    def __init__(
        self, manifest: dict[str, Any] | None, parquet: dict[str, pl.DataFrame]
    ) -> None:
        self.objects: dict[str, Any] = dict(parquet)
        if manifest is not None:
            self.objects[f"snap/{MANIFEST_FILENAME}"] = manifest

    def key_exists(self, key: str, bucket: str) -> bool:  # noqa: ARG002
        return key in self.objects

    def read_json(self, key: str, bucket: str) -> dict[str, Any]:  # noqa: ARG002
        return self.objects[key]

    def list_keys(self, prefix: str, bucket: str) -> list[str]:  # noqa: ARG002
        return [key for key in self.objects if key.startswith(prefix)]

    def read_parquet(self, key: str, bucket: str) -> pl.DataFrame:  # noqa: ARG002
        return self.objects[key]


def price_rows(appids: list[int], snapshot_id: str = SNAPSHOT) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "appid": appids,
            "snapshot_id": [snapshot_id] * len(appids),
            "final_price_cents": [100] * len(appids),
        }
    )


def manifest(records: int) -> dict[str, Any]:
    return {
        "source": "store_prices",
        "snapshot_id": SNAPSHOT,
        "collected_at": COLLECTED_AT,
        "records": records,
        "api_total": records,
        "skipped": 0,
    }


def silver() -> SilverStorePriceIntervalsOperator:
    return SilverStorePriceIntervalsOperator(task_id="silver", snapshot_prefix="snap")


def test_snapshot_load_dedupes_retries_and_ignores_other_snapshots() -> None:
    s3 = FakeSnapshotS3(
        manifest(3),
        {
            "snap/chunk_a.parquet": price_rows([1, 2]),
            # consumer 재처리로 같은 메시지가 다시 저장된 파일
            "snap/chunk_b.parquet": price_rows([2, 3]),
            # 이전 실행에서 넘어온 다른 스냅샷 메시지
            "snap/chunk_c.parquet": price_rows([9], snapshot_id="20261121T183000"),
        },
    )

    loaded_manifest, snapshot = silver().load_snapshot(s3)  # type: ignore[arg-type]

    assert loaded_manifest["records"] == 3
    assert sorted(snapshot["app_id"].to_list()) == [1, 2, 3]


def test_snapshot_without_manifest_or_with_count_mismatch_is_rejected() -> None:
    with pytest.raises(FileNotFoundError, match="manifest 없음"):
        silver().load_snapshot(
            FakeSnapshotS3(None, {"snap/a.parquet": price_rows([1])})
        )  # type: ignore[arg-type]

    with pytest.raises(FileNotFoundError, match="parquet 없음"):
        silver().load_snapshot(FakeSnapshotS3(manifest(1), {}))  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="manifest=3, 적재된 고유 appid=2"):
        silver().load_snapshot(
            FakeSnapshotS3(manifest(3), {"snap/a.parquet": price_rows([1, 2])})
        )  # type: ignore[arg-type]


def test_sudden_drop_blocks_interval_update() -> None:
    operator = silver()

    operator._require_coverage("fact_price_intervals", previous=1000, current=900)
    with pytest.raises(ValueError, match="건수 급감"):
        operator._require_coverage("fact_price_intervals", previous=1000, current=899)

    # 첫 적재(직전 0건)는 비교 대상이 없으므로 통과
    operator._require_coverage("fact_price_intervals", previous=0, current=1)


def test_already_applied_snapshot_is_skipped_and_older_snapshot_is_rejected() -> None:
    operator = silver()

    applied = MagicMock()
    applied.fetchone.return_value = (1,)
    assert operator._already_applied(applied, manifest(1)) is True  # type: ignore[arg-type]

    newer_exists = MagicMock()
    newer_exists.fetchone.side_effect = [
        None,
        ("20261123T183000", COLLECTED_AT + 86400),
    ]
    with pytest.raises(ValueError, match="오래된 스냅샷"):
        operator._already_applied(newer_exists, manifest(1))  # type: ignore[arg-type]

    first_time = MagicMock()
    first_time.fetchone.side_effect = [None, None]
    assert operator._already_applied(first_time, manifest(1)) is False  # type: ignore[arg-type]


def test_price_interval_spec_tracks_price_fields_only() -> None:
    assert "review_count" not in PRICE_INTERVALS.value_columns
    assert PRICE_INTERVALS.timestamp_columns == {"discount_end_at"}


def test_invalid_min_coverage_is_rejected() -> None:
    with pytest.raises(ValueError, match="min_coverage"):
        SilverStorePriceIntervalsOperator(
            task_id="bad", snapshot_prefix="p", min_coverage=0
        )


# ── Query 전체 순회 ──────────────────────────────────────


def query_page(items: list[dict[str, Any]], total: int) -> dict[str, Any]:
    return {
        "response": {
            "store_items": items,
            "metadata": {"total_matching_records": total},
        }
    }


def query_hook(pages: list[dict[str, Any]]) -> SteamApiHook:
    hook = SteamApiHook(api_key="secret")
    hook.get_query = MagicMock(side_effect=pages)  # type: ignore[method-assign]
    hook._wait = MagicMock()  # type: ignore[method-assign]
    return hook


def test_query_iteration_stops_at_total() -> None:
    hook = query_hook(
        [query_page([store_item(1), store_item(2)], 3), query_page([store_item(3)], 3)]
    )

    pages = list(hook.iter_store_query_pages("q", {}, {}, {}, page_size=2))

    assert [len(items) for items, _ in pages] == [2, 1]
    starts = [call.kwargs["start"] for call in hook.get_query.call_args_list]  # type: ignore[attr-defined]
    assert starts == [0, 2]


def test_query_iteration_fails_on_empty_page_before_total() -> None:
    hook = query_hook([query_page([store_item(1)], 3), query_page([], 3)])

    with pytest.raises(RuntimeError, match="빈 페이지: query=q, start=1, total=3"):
        list(hook.iter_store_query_pages("q", {}, {}, {}, page_size=1))


# ── API key 가림 ──────────────────────────────────────


def test_api_key_is_masked_in_logs_and_exceptions(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    hook = SteamApiHook(api_key="SECRETKEY123")

    def fail(url: str, params: dict[str, Any], timeout: int) -> None:  # noqa: ARG001
        raise requests.ConnectionError(f"failed {url}?key={params['key']}")

    monkeypatch.setattr(steam_api_module.requests, "get", fail)

    with (
        caplog.at_level(logging.INFO),
        pytest.raises(requests.ConnectionError) as error,
    ):
        hook._request("https://api.example/x", {"key": "SECRETKEY123", "appid": 1})

    assert "SECRETKEY123" not in str(error.value)
    assert error.value.__cause__ is None and error.value.__suppress_context__, (
        "원본 예외 연결 제거"
    )
    assert "SECRETKEY123" not in caplog.text
    assert "'key': '***'" in caplog.text


def test_http_error_message_has_no_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    hook = SteamApiHook(api_key="SECRETKEY123")
    response = MagicMock(ok=False, status_code=403, text="forbidden")
    monkeypatch.setattr(steam_api_module.requests, "get", lambda *a, **kw: response)

    with pytest.raises(requests.HTTPError) as error:
        hook._request("https://api.example/x", {"key": "SECRETKEY123"})

    assert "status=403" in str(error.value)
    assert "SECRETKEY123" not in str(error.value)
