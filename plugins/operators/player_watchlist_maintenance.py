"""
동접자 관리 목록(player_watchlist) 유지 오퍼레이터

- SyncPlayerWatchlistOperator: dim_apps 신규 게임 등록, 우선 스캔 표시
- RebalancePlayerWatchlistOperator: 시간당 호출 예산 N으로 매시 수집 대상 재배분

고정 대상(first_top100_at 있음 또는 source = 'manual')은 예산과 무관하게 항상 활성
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from psycopg2.extensions import cursor as Cursor

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 예산·저조 판단과 무관하게 항상 매시 수집하는 대상
PINNED_CONDITION = "(first_top100_at IS NOT NULL OR source = 'manual')"

MaintenanceSummary = dict[str, int]


class _WatchlistOperator(BaseOperator):
    """
    단일 트랜잭션 실행 공통 처리

    :param postgres_conn_id: Airflow에 등록한 PostgreSQL 연결 ID
    """

    def __init__(self, postgres_conn_id: str = "analytics_db", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    def run(self, cursor: Cursor) -> MaintenanceSummary:
        """트랜잭션 안에서 수행할 작업 (하위 클래스 구현)"""
        raise NotImplementedError

    def execute(self, context: Context) -> MaintenanceSummary:  # noqa: ARG002
        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id).get_conn()
        try:
            with conn, conn.cursor() as cursor:
                summary = self.run(cursor)
        finally:
            conn.close()

        self.log.info("관리 목록 갱신 완료: %s", summary)
        return summary


class SyncPlayerWatchlistOperator(_WatchlistOperator):
    """
    스캔 전 관리 목록 동기화

    - dim_apps 게재 게임 중 관리 목록에 없는 게임을 즉시 스캔 대상으로 등록
    - 최근 게재·할인 시작·리뷰 수 급증 게임은 간격과 무관하게 이번 스캔에 우선 포함
    - 최초 카탈로그 적재처럼 직전 상태가 없는 변화는 우선 대상에서 제외

    :param lookback_hours: 신규·변경 판단 기간 (가격 수집 1회분을 포함하도록 24시간)
    :param review_jump_min: 리뷰 수 급증 판단 최소 증가량
    """

    def __init__(
        self,
        lookback_hours: int = 24,
        review_jump_min: int = 20,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.lookback_hours = lookback_hours
        self.review_jump_min = review_jump_min

    def run(self, cursor: Cursor) -> MaintenanceSummary:
        cursor.execute(
            "INSERT INTO player_watchlist (app_id, source, next_scan_at) "
            "SELECT app_id, 'scan', now() FROM dim_apps WHERE is_listed "
            "ON CONFLICT (app_id) DO NOTHING"
        )
        registered = cursor.rowcount

        # 신규 게재는 위 등록으로 이미 즉시 스캔 대상이지만 우선순위를 주기 위해 함께 표시
        # 최초 카탈로그 적재분과 첫 가격 구간은 실제 변화가 아니므로 직전 상태가 있는 경우만 포함
        cursor.execute(
            f"""
            WITH priority AS (
                SELECT app_id FROM dim_apps
                WHERE is_listed AND first_seen_at >= now() - %(lookback)s * INTERVAL '1 hour'
                  AND first_seen_at > (SELECT min(first_seen_at) FROM dim_apps)
                UNION
                SELECT cur.app_id
                FROM fact_price_intervals AS cur
                JOIN fact_price_intervals AS prev
                  ON prev.app_id = cur.app_id AND prev.valid_to = cur.valid_from
                WHERE cur.valid_to IS NULL AND cur.discount_pct > 0 AND prev.discount_pct = 0
                  AND cur.valid_from >= now() - %(lookback)s * INTERVAL '1 hour'
                UNION
                SELECT cur.app_id
                FROM fact_review_summary_intervals AS cur
                JOIN fact_review_summary_intervals AS prev
                  ON prev.app_id = cur.app_id AND prev.valid_to = cur.valid_from
                WHERE cur.valid_to IS NULL
                  AND cur.valid_from >= now() - %(lookback)s * INTERVAL '1 hour'
                  AND cur.review_count - prev.review_count >= %(jump)s
            )
            UPDATE player_watchlist AS w
            SET scan_priority = TRUE, next_scan_at = LEAST(w.next_scan_at, now()), updated_at = now()
            FROM priority AS p
            WHERE w.app_id = p.app_id AND NOT w.is_active AND NOT {PINNED_CONDITION}
            """,  # noqa: S608 (상수 조건)
            {"lookback": self.lookback_hours, "jump": self.review_jump_min},
        )
        prioritized = cursor.rowcount

        return {"registered": registered, "prioritized": prioritized}


class RebalancePlayerWatchlistOperator(_WatchlistOperator):
    """
    호출 예산 기준 매시 수집 대상 재배분

    - 활성 게임의 recent_peak는 최근 peak_window_days 동안 매시 수집 최댓값으로 갱신
    - 고정 대상이 아닌 게임 중 recent_peak 상위 budget개(min_peak 이상)를 활성화
    - 비활성으로 바뀐 게임은 다음 날부터 1일 간격 스캔으로 복귀

    :param budget: 예산 대상 게임 수 (Jinja 템플릿 가능)
    :param min_peak: 예산 대상 최소 recent_peak
    :param peak_window_days: recent_peak 계산 기간
    """

    template_fields = ("budget",)

    def __init__(
        self,
        budget: str | int = 2000,
        min_peak: int = 1,
        peak_window_days: int = 7,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.budget = budget
        self.min_peak = min_peak
        self.peak_window_days = peak_window_days

    def run(self, cursor: Cursor) -> MaintenanceSummary:
        budget = int(self.budget)
        if budget < 0:
            raise ValueError(f"관리 목록 예산은 0 이상이어야 합니다: budget={budget!r}")

        # 활성 게임은 여러 시각의 최댓값, 스캔 게임은 하루 1회 값이라 활성 쪽이 유리해
        # 경계 게임이 매일 들어왔다 나가는 반복을 줄임
        cursor.execute(
            """
            UPDATE player_watchlist AS w
            SET recent_peak = COALESCE((
                    SELECT max(f.player_count) FROM fact_player_counts AS f
                    WHERE f.app_id = w.app_id
                      AND f.collected_at >= now() - %s * INTERVAL '1 day'
                ), 0),
                updated_at = now()
            WHERE w.is_active
            """,
            (self.peak_window_days,),
        )

        cursor.execute(
            f"""
            WITH selected AS (
                SELECT app_id FROM player_watchlist
                WHERE NOT {PINNED_CONDITION} AND recent_peak >= %(min_peak)s
                ORDER BY recent_peak DESC, app_id
                LIMIT %(budget)s
            ),
            desired AS (
                SELECT w.app_id,
                       ({PINNED_CONDITION} OR s.app_id IS NOT NULL) AS should_be_active
                FROM player_watchlist AS w
                LEFT JOIN selected AS s ON s.app_id = w.app_id
            )
            UPDATE player_watchlist AS w
            SET is_active = d.should_be_active,
                next_scan_at = CASE WHEN d.should_be_active THEN w.next_scan_at
                                    ELSE now() + INTERVAL '1 day' END,
                scan_interval_days = CASE WHEN d.should_be_active THEN w.scan_interval_days ELSE 1 END,
                consecutive_low_scans = CASE WHEN d.should_be_active THEN w.consecutive_low_scans ELSE 0 END,
                updated_at = now()
            FROM desired AS d
            WHERE w.app_id = d.app_id AND w.is_active IS DISTINCT FROM d.should_be_active
            RETURNING d.should_be_active
            """,  # noqa: S608 (상수 조건)
            {"min_peak": self.min_peak, "budget": budget},
        )
        changes = [row[0] for row in cursor.fetchall()]

        cursor.execute(
            f"SELECT count(*) FILTER (WHERE {PINNED_CONDITION}), "  # noqa: S608
            f"count(*) FILTER (WHERE is_active AND NOT {PINNED_CONDITION}) "
            "FROM player_watchlist"
        )
        pinned, budgeted = cursor.fetchone()

        return {
            "activated": sum(changes),
            "deactivated": len(changes) - sum(changes),
            "pinned": pinned,
            "budget_active": budgeted,
            "budget": budget,
        }
