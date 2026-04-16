"""
Silver fact_review_daily 집계 DAG

Silver S3 리뷰 parquet → fact_review_daily 일별 집계 UPSERT.
steam_silver_03_reviews (리뷰 Silver 적재) 완료 후 실행.

적재 결과 활용:
  fact_review_daily + fact_price_history JOIN
  → 할인 기간 vs 비할인 기간 리뷰 수 A/B 비교
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.silver_review_daily_to_fact import SilverReviewDailyToFactOperator

with DAG(
    dag_id="steam_silver_05_review_daily",
    description="Silver S3 리뷰 → fact_review_daily 일별 집계 적재",
    schedule="0 10 * * *",   # silver_03_reviews(09:00) 완료 후
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "silver", "fact"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    load_review_daily = SilverReviewDailyToFactOperator(
        task_id="load_review_daily",
    )
