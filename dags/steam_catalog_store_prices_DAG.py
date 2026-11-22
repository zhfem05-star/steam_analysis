"""
Steam KR 가격·리뷰 요약 정기 수집 DAG

IStoreQueryService/Query 전체 게임 순회로 KR 가격과 리뷰 요약을 매일 스냅샷으로 수집한다.
2026-11-22 실측 기준 202회 호출, 약 6분.

처리 순서:
  1. produce_store_prices : Query 전체 순회 → Kafka(steam.prices) 발행, manifest 기록
  2. publish_store_prices : Kafka → S3(steam-raw) parquet 저장
  3. load_price_intervals : manifest 검증 후 가격·리뷰 요약 변경 구간 갱신

저장 경로: store_prices/{ts_nodash}/
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.kafka_topic_to_s3 import KafkaTopicToS3Operator
from operators.silver_store_snapshot_to_db import SilverStorePriceIntervalsOperator
from operators.steam_store_snapshot_to_kafka import SteamStorePricesToKafkaOperator

_TOPIC = "steam.prices"
_SNAPSHOT_PREFIX = "store_prices/{{ ts_nodash }}"

with DAG(
    dag_id="steam_catalog_02_store_prices",
    description="Query → Kafka → S3 → 가격·리뷰 요약 변경 구간 (KR, 일별)",
    # Steam 할인은 보통 미국 서부 오전 10시(17~18시 UTC)에 바뀌므로 그 이후 수집
    schedule="30 18 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "catalog"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:
    produce_store_prices = SteamStorePricesToKafkaOperator(
        task_id="produce_store_prices",
        kafka_topic=_TOPIC,
        snapshot_id="{{ ts_nodash }}",
        snapshot_prefix=_SNAPSHOT_PREFIX,
        country_code="KR",
    )

    # producer가 중간에 실패해도 이미 발행한 메시지는 저장하도록 all_done
    publish_store_prices = KafkaTopicToS3Operator(
        task_id="publish_store_prices",
        trigger_rule="all_done",
        kafka_topic=_TOPIC,
        consumer_group_id="steam-prices-to-s3",
        s3_key_prefix=_SNAPSHOT_PREFIX,
    )

    # producer·consumer가 모두 성공한 완료 스냅샷만 반영
    load_price_intervals = SilverStorePriceIntervalsOperator(
        task_id="load_price_intervals",
        snapshot_prefix=_SNAPSHOT_PREFIX,
    )

    produce_store_prices >> publish_store_prices >> load_price_intervals
