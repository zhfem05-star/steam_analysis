"""
Steam 동접자 수 수집 DAG

tracked_games 테이블에 누적된 app_id 목록을 기준으로
6시간 간격으로 각 게임의 동접자 수를 수집해 MinIO(S3) steam-raw 버킷에 저장한다.
하루 4회 스냅샷(00:00 / 06:00 / 12:00 / 18:00 UTC)을 통해 일평균 동접자를 산출할 수 있다.

저장 경로: player_counts/{YYYYMMDD_HHMM}_player_counts.json
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from callbacks.slack_callback import slack_fail_alert
from operators.steam_players_to_s3 import SteamPlayersToS3Operator


def _fetch_tracked_app_ids(**_):
    """tracked_games 테이블에서 추적 중인 appid 리스트를 반환."""
    hook = PostgresHook(postgres_conn_id="analytics_db")
    rows = hook.get_records("SELECT appid FROM tracked_games")
    return [row[0] for row in rows]


with DAG(
    dag_id="bronze_player_count_extract",
    description="Steam API → MinIO(steam-raw) 게임별 동접자 수 수집 (6시간 간격)",
    schedule="0 0,6,12,18 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "extract"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    # tracked_games DB에서 추적 대상 appid 리스트 조회 → XCom push
    fetch_tracked_ids = PythonOperator(
        task_id="fetch_tracked_ids",
        python_callable=_fetch_tracked_app_ids,
    )

    # XCom에서 appid 리스트를 받아 동접자 수집 후 S3 저장
    collect_player_counts = SteamPlayersToS3Operator(
        task_id="collect_player_counts",
        app_ids_dag_id="bronze_player_count_extract",
        app_ids_task_id="fetch_tracked_ids",
        s3_key="player_counts/{{ execution_date.strftime('%Y%m%d_%H%M') }}_player_counts.json",
    )

    fetch_tracked_ids >> collect_player_counts
