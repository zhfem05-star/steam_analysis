"""
Steam 게임 목록 정기 수집 DAG

GetAppList 전체 게임(DLC·소프트웨어·영상·하드웨어 제외)을 매일 스냅샷으로 수집한다.

처리 순서:
  1. produce_app_list  : GetAppList 전체 순회 → Kafka(steam.app_list) 발행, manifest 기록
  2. publish_app_list  : Kafka → S3(steam-raw) parquet 저장
  3. load_dim_apps     : manifest 검증 후 dim_apps upsert, 목록에서 사라진 게임 is_listed=FALSE

저장 경로: app_list/{ts_nodash}/
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.kafka_topic_to_s3 import KafkaTopicToS3Operator
from operators.silver_store_snapshot_to_db import SilverAppCatalogOperator
from operators.steam_store_snapshot_to_kafka import SteamAppListToKafkaOperator

_TOPIC = "steam.app_list"
_SNAPSHOT_PREFIX = "app_list/{{ ts_nodash }}"

with DAG(
    dag_id="steam_catalog_01_app_list",
    description="GetAppList → Kafka → S3 → dim_apps 게임 목록 일별 스냅샷",
    schedule="0 6 * * *",  # 매일 06:00 UTC
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "catalog"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:
    produce_app_list = SteamAppListToKafkaOperator(
        task_id="produce_app_list",
        kafka_topic=_TOPIC,
        snapshot_id="{{ ts_nodash }}",
        snapshot_prefix=_SNAPSHOT_PREFIX,
    )

    # producer가 중간에 실패해도 이미 발행한 메시지는 저장하도록 all_done
    publish_app_list = KafkaTopicToS3Operator(
        task_id="publish_app_list",
        trigger_rule="all_done",
        kafka_topic=_TOPIC,
        consumer_group_id="steam-app-list-to-s3",
        s3_key_prefix=_SNAPSHOT_PREFIX,
    )

    # producer·consumer가 모두 성공한 완료 스냅샷만 반영
    load_dim_apps = SilverAppCatalogOperator(
        task_id="load_dim_apps",
        snapshot_prefix=_SNAPSHOT_PREFIX,
    )

    produce_app_list >> publish_app_list >> load_dim_apps
