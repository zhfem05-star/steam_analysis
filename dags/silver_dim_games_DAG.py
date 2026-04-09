"""
Silver dim 테이블 적재 DAG

Bronze appdetails parquet → dim_games / dim_genres / dim_game_genres UPSERT
04번 Bronze DAG(09:00 UTC) 완료 후 실행.
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.silver_appdetails_to_dim import SilverAppDetailsToDimOperator


with DAG(
    dag_id="steam_silver_01_dim_games",
    description="Bronze appdetails → dim_games / dim_genres / dim_game_genres UPSERT",
    schedule="0 10 * * *",     # 매일 10:00 UTC (04번 Bronze DAG 완료 후)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "silver", "dim"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    load_dim_games = SilverAppDetailsToDimOperator(
        task_id="load_dim_games",
        s3_key_prefix="appdetails/{{ execution_date.strftime('%Y%m%d') }}",
    )
