"""
Silver fact_concurrent_players 적재 DAG

Bronze player_counts JSON → fact_concurrent_players UPSERT
03번 Bronze DAG(08:00 UTC) 완료 후 실행.
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.silver_players_to_fact import SilverPlayersToFactOperator


with DAG(
    dag_id="steam_silver_04_fact_players",
    description="Bronze player_counts JSON → fact_concurrent_players UPSERT",
    schedule="0 9 * * *",      # 매일 09:00 UTC (03번 Bronze DAG 완료 후)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "silver", "fact"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    load_fact_players = SilverPlayersToFactOperator(
        task_id="load_fact_players",
        s3_key_prefix="player_counts/{{ execution_date.strftime('%Y%m%d') }}",
    )
