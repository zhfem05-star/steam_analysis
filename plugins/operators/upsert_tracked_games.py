"""
할인 게임 app_id → tracked_games 테이블 UPSERT 오퍼레이터

XCom에서 appid 리스트를 받아 analytics_postgres의 tracked_games 테이블에
UPSERT한다. 처음 등장한 게임은 INSERT, 이미 있는 게임은 last_discounted만 갱신.
"""

from __future__ import annotations

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class UpsertTrackedGamesOperator(BaseOperator):
    """
    XCom에서 appid 리스트를 받아 tracked_games 테이블에 UPSERT하는 오퍼레이터.

    :param app_ids_task_id: appid 리스트를 push한 태스크의 task_id
    :param postgres_conn_id: Airflow Postgres Connection ID
    """

    def __init__(
        self,
        app_ids_task_id: str,
        postgres_conn_id: str = "analytics_db",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.app_ids_task_id = app_ids_task_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        app_ids: list[int] = context["ti"].xcom_pull(task_ids=self.app_ids_task_id)

        if not app_ids:
            raise ValueError("XCom에서 appid 리스트를 가져오지 못했습니다.")

        today = context["execution_date"].date()

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(
            """
                INSERT INTO tracked_games (appid, first_seen, last_discounted)
                VALUES (%s, %s, %s)
                ON CONFLICT (appid) DO UPDATE
                    SET last_discounted = EXCLUDED.last_discounted
            """,
                [(appid, today, today) for appid in app_ids],
        )
        
        conn.commit()

        self.log.info("tracked_games UPSERT 완료: %d개", len(app_ids))
