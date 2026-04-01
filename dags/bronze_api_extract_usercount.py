"""
Steam 동접자 수 수집 DAG

tracked_games 컨트롤 테이블의 is_active=TRUE, collect_players=TRUE인 게임을 대상으로
6시간 간격으로 동접자 수를 수집해 MinIO(S3) steam-raw 버킷에 저장한다.
하루 4회 스냅샷(00:00 / 06:00 / 12:00 / 18:00 UTC)을 통해 일평균 동접자를 산출할 수 있다.

저장 경로: player_counts/{YYYYMMDD_HHMM}_player_counts.json
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.steam_players_to_s3 import SteamPlayersToS3Operator


with DAG(
    dag_id="steam_bronze_03_player_count_extract",
    description="Steam API → MinIO(steam-raw) 게임별 동접자 수 수집 (6시간 간격)",
    schedule="0 0,6,12,18 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "extract"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    # tracked_games 테이블에서 app_id 조회 → 동접자 수집 후 S3 저장
    collect_player_counts = SteamPlayersToS3Operator(
        task_id="collect_player_counts",
        s3_key="player_counts/{{ execution_date.strftime('%Y%m%d_%H%M') }}_player_counts.json",
    )
