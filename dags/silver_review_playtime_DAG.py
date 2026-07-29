"""
Silver 리뷰 플레이타임 일별 집계 DAG

Silver S3 리뷰 parquet → fact_review_playtime_daily UPSERT
할인 구간 vs 비할인 구간 플레이타임 A/B 비교 분석용.
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.silver_review_playtime_to_fact import SilverReviewPlaytimeToFactOperator


with DAG(
    dag_id="steam_silver_06_review_playtime",
    description="Silver 리뷰 parquet → fact_review_playtime_daily (플레이타임 일별 집계)",
    schedule="0 10 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "silver", "fact", "playtime"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    aggregate_playtime = SilverReviewPlaytimeToFactOperator(
        task_id="aggregate_playtime",
    )
