"""
Silver 리뷰 파티셔닝 DAG

Bronze reviews parquet → Silver S3 파티셔닝 저장
02번 Bronze DAG(08:00 UTC) 완료 후 실행.

Silver 저장 경로:
  s3://steam-silver/reviews/appid={id}/year={Y}/month={M}/{filename}.parquet
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.silver_reviews_to_s3 import SilverReviewsToS3Operator


with DAG(
    dag_id="steam_silver_03_fact_reviews",
    description="Bronze reviews parquet → S3 Silver 파티셔닝 (appid/year/month)",
    schedule="0 9 * * *",      # 매일 09:00 UTC (02번 Bronze DAG 완료 후)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "silver", "fact", "reviews"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    partition_reviews = SilverReviewsToS3Operator(
        task_id="partition_reviews",
        bronze_prefix="reviews/{{ execution_date.strftime('%Y%m%d') }}",
    )
