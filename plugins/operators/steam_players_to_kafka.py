"""
Steam 동접자 수집 → Kafka 발행 오퍼레이터

- 매시 수집: top 100 호출 1회 + 관리 목록 활성 게임 중 top 100에 없는 게임 개별 호출
- 일별 스캔: 스캔 일정이 도래한 비활성 게임 개별 호출 (1회 최대 건수 제한)
- 개별 호출은 token bucket(기본 초당 5건)으로 요청률을 제한하고 429에 대응
- 전송 확인 후 manifest 기록은 StoreSnapshotToKafkaOperator 공통 흐름을 따름

수집 실패 처리:
  - 데이터 없음(404·result 42)은 not_found, 네트워크·5xx는 error로 기록하고 계속 진행
  - error가 연속 한도 또는 전체 비율 한도를 넘으면 실행 실패 (manifest 미기록)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hooks.steam_api import SteamApiHook
from utils.player_watchlist import (
    PLAYER_STATUS_ERROR,
    SOURCE_INDIVIDUAL,
    SOURCE_SCAN,
    PlayerCountResult,
    PlayerSource,
    individual_message,
    individual_targets,
    top100_messages,
)
from utils.rate_limit import RateLimitedCaller, TokenBucket

from operators.steam_store_snapshot_to_kafka import (
    SnapshotMessage,
    SnapshotPage,
    StoreSnapshotToKafkaOperator,
)


class PlayerFetchToKafkaOperator(StoreSnapshotToKafkaOperator):
    """
    개별 동접자 호출 공통 처리

    :param rate_per_sec: 개별 호출 초당 요청 수
    :param chunk_size: Kafka flush 단위 호출 수
    :param max_error_ratio: 허용 수집 실패 비율 (최소 50건 이상 호출 시 판단)
    :param max_consecutive_errors: 허용 연속 수집 실패 수
    :param postgres_conn_id: Airflow에 등록한 PostgreSQL 연결 ID
    """

    def __init__(
        self,
        rate_per_sec: float = 5.0,
        chunk_size: int = 200,
        max_error_ratio: float = 0.2,
        max_consecutive_errors: int = 20,
        postgres_conn_id: str = "analytics_db",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if chunk_size < 1:
            raise ValueError(f"chunk_size는 1 이상이어야 합니다: chunk_size={chunk_size!r}")

        self.rate_per_sec = rate_per_sec
        self.chunk_size = chunk_size
        self.max_error_ratio = max_error_ratio
        self.max_consecutive_errors = max_consecutive_errors
        self.postgres_conn_id = postgres_conn_id

    def _fetch_one(self, api_hook: SteamApiHook, caller: RateLimitedCaller, appid: int) -> PlayerCountResult:
        """
        한 게임 조회. 429 외 요청 실패는 error 결과로 변환

        :raises RuntimeError: 연속 429 한도 초과
        """
        try:
            return caller.call(lambda: api_hook.fetch_current_players(appid))
        except requests.RequestException as error:
            self.log.warning(
                "동접자 조회 실패: source=%s  appid=%s  error=%s",
                self.source,
                appid,
                type(error).__name__,
                exc_info=self.log.isEnabledFor(logging.DEBUG),
            )
            return PlayerCountResult(None, PLAYER_STATUS_ERROR)

    def iter_individual_pages(
        self,
        api_hook: SteamApiHook,
        appids: list[int],
        collected_at: int,
        source: PlayerSource,
    ) -> Iterator[SnapshotPage]:
        """
        개별 호출 결과를 chunk_size 단위 페이지로 순회

        :raises RuntimeError: 연속 실패 또는 실패 비율 한도 초과
        """
        caller = RateLimitedCaller(TokenBucket(self.rate_per_sec))
        errors = 0
        consecutive_errors = 0

        for start in range(0, len(appids), self.chunk_size):
            messages: list[SnapshotMessage] = []
            for appid in appids[start : start + self.chunk_size]:
                result = self._fetch_one(api_hook, caller, appid)

                if result.status == PLAYER_STATUS_ERROR:
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors > self.max_consecutive_errors:
                        raise RuntimeError(
                            f"동접자 연속 수집 실패로 중단: source={self.source}, "
                            f"연속={consecutive_errors}, 마지막 appid={appid}"
                        )
                else:
                    consecutive_errors = 0

                messages.append(
                    individual_message(
                        appid,
                        result,
                        self.snapshot_id,
                        collected_at,
                        int(time.time()),
                        source,
                    )
                )

            self.log.info(
                "개별 동접자 %d/%d건 조회 (실패 %d, 429 %d)",
                min(start + self.chunk_size, len(appids)),
                len(appids),
                errors,
                caller.throttled,
            )
            yield SnapshotPage(messages, skipped=0, api_total=None)

        enough_samples = len(appids) >= 50
        if enough_samples and errors > len(appids) * self.max_error_ratio:
            raise RuntimeError(
                f"동접자 수집 실패 비율 초과: source={self.source}, 실패={errors}, "
                f"대상={len(appids)}, 허용 비율={self.max_error_ratio}"
            )

    def _query_ids(self, sql: str, parameters: tuple[Any, ...] = ()) -> list[int]:
        rows = PostgresHook(postgres_conn_id=self.postgres_conn_id).get_records(sql, parameters=parameters)
        return [int(row[0]) for row in rows]


class SteamPlayerCountsToKafkaOperator(PlayerFetchToKafkaOperator):
    """
    매시 동접자 수집 (top 100 + 관리 목록 활성 게임)

    - 같은 시각 수집분은 slot_at(정시)으로 묶어 재실행 시 같은 키로 적재
    - 늦게 재시도된 실행이 현재 값을 과거 슬롯에 기록하지 않도록 지연 한도 검사

    :param slot_at: 수집 슬롯 Unix 초 (Jinja 템플릿, 예: "{{ data_interval_end.int_timestamp }}")
    :param max_slot_lag_sec: 슬롯 시각과 실제 실행 시각의 허용 차이
    """

    source = "players"
    template_fields = (*StoreSnapshotToKafkaOperator.template_fields, "slot_at")

    def __init__(
        self,
        slot_at: str | int,
        max_slot_lag_sec: int = 3600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.slot_at = slot_at
        self.max_slot_lag_sec = max_slot_lag_sec

    def _validated_slot(self, started_at: int) -> int:
        """
        :raises ValueError: 슬롯이 실행 시각보다 미래이거나 지연 한도 초과
        """
        slot_at = int(self.slot_at)
        lag = started_at - slot_at
        if lag < -300 or lag > self.max_slot_lag_sec:
            raise ValueError(
                f"수집 슬롯과 실행 시각 차이가 허용 범위 밖: slot_at={slot_at}, "
                f"실행={started_at}, 차이={lag}초, 허용 최대={self.max_slot_lag_sec}초"
            )
        return slot_at

    def iter_snapshot_pages(
        self,
        api_hook: SteamApiHook,
        collected_at: int,
    ) -> Iterator[SnapshotPage]:
        slot_at = self._validated_slot(collected_at)

        ranks = api_hook.get_top_concurrent_players()
        top = top100_messages(ranks, self.snapshot_id, slot_at, int(time.time()))
        yield SnapshotPage(list(top), skipped=0, api_total=len(ranks))

        active_ids = self._query_ids("SELECT app_id FROM player_watchlist WHERE is_active")
        targets = individual_targets(active_ids, [message["appid"] for message in top])
        self.log.info(
            "매시 수집 대상: top100=%d  활성 관리 목록=%d  개별 호출=%d",
            len(top),
            len(active_ids),
            len(targets),
        )
        yield from self.iter_individual_pages(api_hook, targets, slot_at, SOURCE_INDIVIDUAL)


class SteamPlayerScanToKafkaOperator(PlayerFetchToKafkaOperator):
    """
    일별 동접자 스캔 (스캔 일정이 도래한 비활성 게임)

    - 우선 스캔 → 리뷰 수 많은 순 → 오래 기다린 순으로 최대 max_targets건
    - 최초 전체 스캔은 한도 때문에 여러 날에 나뉘어 진행

    :param max_targets: 1회 최대 스캔 수 (Jinja 템플릿 가능)
    """

    source = "player_scan"
    template_fields = (*StoreSnapshotToKafkaOperator.template_fields, "max_targets")

    def __init__(self, max_targets: str | int = 30_000, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.max_targets = max_targets

    def iter_snapshot_pages(
        self,
        api_hook: SteamApiHook,
        collected_at: int,
    ) -> Iterator[SnapshotPage]:
        max_targets = int(self.max_targets)
        if max_targets < 1:
            raise ValueError(f"스캔 최대 건수는 1 이상이어야 합니다: max_targets={max_targets!r}")

        targets = self._query_ids(
            """
            SELECT w.app_id
            FROM player_watchlist AS w
            JOIN dim_apps AS d ON d.app_id = w.app_id AND d.is_listed
            LEFT JOIN fact_review_summary_intervals AS r
                ON r.app_id = w.app_id AND r.valid_to IS NULL
            WHERE NOT w.is_active AND w.next_scan_at <= to_timestamp(%s)
            ORDER BY w.scan_priority DESC, r.review_count DESC NULLS LAST, w.next_scan_at, w.app_id
            LIMIT %s
            """,
            (collected_at, max_targets),
        )
        self.log.info("스캔 대상 %d건 (최대 %d건)", len(targets), max_targets)
        yield from self.iter_individual_pages(api_hook, targets, collected_at, SOURCE_SCAN)
