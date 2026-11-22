"""
동접자 스냅샷(S3) → PostgreSQL 적재 오퍼레이터

- 매시 수집: fact_player_counts 적재, top 100 진입 게임을 player_watchlist에 고정 등록
- 일별 스캔: player_watchlist의 최근 값·스캔 간격·다음 스캔 시각 갱신
- manifest 검증과 적용 기록은 SilverStoreSnapshotOperator 공통 흐름을 따름
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from psycopg2.extensions import cursor as Cursor

import polars as pl
from utils.player_watchlist import (
    PLAYER_STATUS_ERROR,
    PLAYER_STATUS_NOT_FOUND,
    PLAYER_STATUS_OK,
    SOURCE_TOP100,
    month_partition,
    plan_next_scan,
)
from utils.store_catalog import SnapshotManifest

from operators.silver_store_snapshot_to_db import (
    ApplySummary,
    SilverStoreSnapshotOperator,
)

FACT_TABLE = "fact_player_counts"

_FACT_COLUMNS = [
    "app_id",
    "collected_at",
    "fetched_at",
    "player_count",
    "status",
    "source",
    "rank",
    "peak_in_game",
]


class SilverPlayerCountsOperator(SilverStoreSnapshotOperator):
    """
    매시 동접자 스냅샷 적재

    - 늦게 재처리한 과거 시각도 적용하되 관리 목록 최근 값은 더 새로운 관측일 때만 갱신
    """

    source = "players"
    reject_older_snapshots = False

    def apply_snapshot(
        self,
        cursor: Cursor,
        manifest: SnapshotManifest,
        snapshot: pl.DataFrame,
    ) -> ApplySummary:
        if snapshot.is_empty():
            raise ValueError(
                f"매시 동접자 스냅샷이 비어 있음: snapshot={manifest['snapshot_id']}"
            )

        self._ensure_partitions(cursor, snapshot["collected_at"].unique().to_list())
        inserted = self._upsert_facts(cursor, snapshot)
        newly_pinned = self._register_top100(cursor, snapshot)
        refreshed = self._refresh_last_observation(cursor, snapshot)

        status_counts = dict(snapshot.group_by("status").len().iter_rows())
        return {
            "source": self.source,
            "snapshot_id": manifest["snapshot_id"],
            "facts": inserted,
            "statuses": status_counts,
            "newly_pinned_top100": newly_pinned,
            "watchlist_refreshed": refreshed,
        }

    @staticmethod
    def _ensure_partitions(cursor: Cursor, collected_at_values: list[int]) -> None:
        """적재 시각이 속한 월 파티션이 없으면 생성"""
        partitions = {month_partition(FACT_TABLE, value) for value in collected_at_values}
        for partition in sorted(partitions):
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {partition.name} PARTITION OF {FACT_TABLE} "  # noqa: S608 (코드 계산 이름)
                "FOR VALUES FROM (%s) TO (%s)",
                (partition.start, partition.end),
            )

    @staticmethod
    def _upsert_facts(cursor: Cursor, snapshot: pl.DataFrame) -> int:
        from psycopg2.extras import execute_values

        rows = list(snapshot.select(_FACT_COLUMNS).iter_rows())
        execute_values(
            cursor,
            f"INSERT INTO {FACT_TABLE} ({', '.join(_FACT_COLUMNS)}) VALUES %s "  # noqa: S608
            "ON CONFLICT (app_id, collected_at) DO UPDATE SET "
            "fetched_at = EXCLUDED.fetched_at, player_count = EXCLUDED.player_count, "
            "status = EXCLUDED.status, source = EXCLUDED.source, "
            "rank = EXCLUDED.rank, peak_in_game = EXCLUDED.peak_in_game",
            rows,
            template="(%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s)",
            page_size=1000,
        )
        return len(rows)

    @staticmethod
    def _register_top100(cursor: Cursor, snapshot: pl.DataFrame) -> int:
        """
        top 100 응답 게임을 고정 수집 대상으로 등록

        - 이미 있으면 first_top100_at은 최초 값을 유지하고 활성화만 보장

        :return: 이번에 처음 고정 대상이 된 게임 수
        """
        from psycopg2.extras import execute_values

        top = snapshot.filter(pl.col("source") == SOURCE_TOP100).select(
            ["app_id", "collected_at"]
        )
        if top.is_empty():
            return 0

        app_ids = top["app_id"].to_list()
        cursor.execute(
            "SELECT count(*) FROM player_watchlist WHERE app_id = ANY(%s) AND first_top100_at IS NOT NULL",
            (app_ids,),
        )
        already_pinned = cursor.fetchone()[0]

        execute_values(
            cursor,
            "INSERT INTO player_watchlist (app_id, source, first_top100_at, is_active) VALUES %s "
            "ON CONFLICT (app_id) DO UPDATE SET "
            "first_top100_at = COALESCE(player_watchlist.first_top100_at, EXCLUDED.first_top100_at), "
            "is_active = TRUE, updated_at = now()",
            list(top.iter_rows()),
            template=f"(%s, '{SOURCE_TOP100}', to_timestamp(%s), TRUE)",
            page_size=1000,
        )
        return len(app_ids) - already_pinned

    @staticmethod
    def _refresh_last_observation(cursor: Cursor, snapshot: pl.DataFrame) -> int:
        """수집 성공·데이터 없음 결과로 관리 목록의 최근 값 갱신 (더 새로운 관측만)"""
        from psycopg2.extras import execute_values

        observed = snapshot.filter(pl.col("status") != PLAYER_STATUS_ERROR).select(
            ["app_id", "player_count", "collected_at"]
        )
        if observed.is_empty():
            return 0

        cursor.execute(
            "CREATE TEMP TABLE IF NOT EXISTS tmp_player_observations "
            "(app_id INTEGER PRIMARY KEY, player_count INTEGER, observed_at BIGINT) ON COMMIT DROP"
        )
        cursor.execute("TRUNCATE tmp_player_observations")
        execute_values(
            cursor,
            "INSERT INTO tmp_player_observations (app_id, player_count, observed_at) VALUES %s",
            list(observed.iter_rows()),
            page_size=1000,
        )
        cursor.execute(
            "UPDATE player_watchlist AS w SET last_player_count = o.player_count, "
            "last_observed_at = to_timestamp(o.observed_at), updated_at = now() "
            "FROM tmp_player_observations AS o WHERE w.app_id = o.app_id "
            "AND (w.last_observed_at IS NULL OR w.last_observed_at < to_timestamp(o.observed_at))"
        )
        return cursor.rowcount


class SilverPlayerScanOperator(SilverStoreSnapshotOperator):
    """
    일별 스캔 결과로 관리 목록 스캔 일정 갱신

    - 간격 규칙은 utils.player_watchlist.plan_next_scan
    - 스캔 값은 recent_peak로 기록해 예산 재배분 정렬에 사용

    :param low_threshold: 저조 판단 기준 동접자 수 (Jinja 템플릿 가능)
    :param max_interval_days: 최대 스캔 간격 일수 (Jinja 템플릿 가능)
    """

    source = "player_scan"
    template_fields = (
        *SilverStoreSnapshotOperator.template_fields,
        "low_threshold",
        "max_interval_days",
    )

    def __init__(
        self,
        low_threshold: str | int = 10,
        max_interval_days: str | int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.low_threshold = low_threshold
        self.max_interval_days = max_interval_days

    def apply_snapshot(
        self,
        cursor: Cursor,
        manifest: SnapshotManifest,
        snapshot: pl.DataFrame,
    ) -> ApplySummary:
        low_threshold = int(self.low_threshold)
        max_interval_days = int(self.max_interval_days)
        if max_interval_days < 1:
            raise ValueError(
                f"최대 스캔 간격은 1일 이상이어야 합니다: max_interval_days={max_interval_days!r}"
            )

        summary: ApplySummary = {
            "source": self.source,
            "snapshot_id": manifest["snapshot_id"],
            "scanned": snapshot.height,
        }
        if snapshot.is_empty():
            return summary

        from psycopg2.extras import execute_values

        app_ids = snapshot["app_id"].to_list()
        cursor.execute(
            "SELECT app_id, scan_interval_days, consecutive_low_scans FROM player_watchlist WHERE app_id = ANY(%s)",
            (app_ids,),
        )
        current_state = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        updates = []
        low_scans = 0
        for app_id, player_count, status in snapshot.select(
            ["app_id", "player_count", "status"]
        ).iter_rows():
            if app_id not in current_state:
                continue
            interval_days, consecutive_low = current_state[app_id]
            schedule = plan_next_scan(
                player_count,
                status,
                interval_days,
                consecutive_low,
                low_threshold=low_threshold,
                max_interval_days=max_interval_days,
            )
            low_scans += schedule.consecutive_low_scans > consecutive_low
            updates.append(
                (
                    app_id,
                    player_count,
                    status,
                    schedule.interval_days,
                    schedule.consecutive_low_scans,
                    schedule.delay_days,
                )
            )

        cursor.execute(
            "CREATE TEMP TABLE IF NOT EXISTS tmp_scan_results (app_id INTEGER PRIMARY KEY, "
            "player_count INTEGER, status TEXT, interval_days INTEGER, consecutive_low INTEGER, "
            "delay_days INTEGER) ON COMMIT DROP"
        )
        cursor.execute("TRUNCATE tmp_scan_results")
        execute_values(
            cursor,
            "INSERT INTO tmp_scan_results VALUES %s",
            updates,
            page_size=1000,
        )
        # 수집 실패(error)는 이전 관측값을 유지하고 일정만 다음 날로 미룸
        cursor.execute(
            "UPDATE player_watchlist AS w SET "
            "last_player_count = CASE WHEN r.status = %(error)s THEN w.last_player_count ELSE r.player_count END, "
            "last_observed_at = CASE WHEN r.status = %(error)s THEN w.last_observed_at ELSE to_timestamp(%(observed_at)s) END, "
            "recent_peak = CASE WHEN r.status = %(error)s THEN w.recent_peak ELSE COALESCE(r.player_count, 0) END, "
            "last_scanned_at = to_timestamp(%(observed_at)s), "
            "scan_interval_days = r.interval_days, "
            "consecutive_low_scans = r.consecutive_low, "
            "next_scan_at = to_timestamp(%(observed_at)s) + r.delay_days * INTERVAL '1 day', "
            "scan_priority = FALSE, updated_at = now() "
            "FROM tmp_scan_results AS r WHERE w.app_id = r.app_id",
            {"error": PLAYER_STATUS_ERROR, "observed_at": manifest["collected_at"]},
        )

        statuses = dict(snapshot.group_by("status").len().iter_rows())
        summary.update(
            {
                "updated": cursor.rowcount,
                "missing_from_watchlist": snapshot.height - len(updates),
                "low": low_scans,
                "ok": statuses.get(PLAYER_STATUS_OK, 0),
                "not_found": statuses.get(PLAYER_STATUS_NOT_FOUND, 0),
                "error": statuses.get(PLAYER_STATUS_ERROR, 0),
            }
        )
        return summary
