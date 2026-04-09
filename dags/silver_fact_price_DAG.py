"""
Silver fact_price_history 적재 DAG

Bronze discount_games JSON → fact_price_history UPSERT
01번 Bronze DAG(08:00 UTC) 완료 후 실행.
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.silver_discount_to_fact_price import SilverDiscountToFactPriceOperator


with DAG(
    dag_id="steam_silver_02_fact_price",
    description="Bronze discount_games JSON → fact_price_history UPSERT",
    schedule="0 9 * * *",      # 매일 09:00 UTC (01번 Bronze DAG 완료 후)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "silver", "fact"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    load_fact_price = SilverDiscountToFactPriceOperator(
        task_id="load_fact_price",
        s3_key_prefix="discount_games/{{ execution_date.strftime('%Y%m%d') }}",
        collected_at="{{ ds }}",
    )
