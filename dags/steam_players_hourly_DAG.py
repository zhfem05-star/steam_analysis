"""
Steam 매시 동접자 수집 DAG

top 100 호출 1회와 관리 목록(player_watchlist) 활성 게임 개별 호출로 매시 동접자를 수집한다.
top 100 응답에 있는 게임은 개별 호출하지 않는다.

처리 순서:
  1. produce_player_counts : top 100 + 활성 게임 개별 호출 → Kafka(steam.players), manifest 기록
  2. publish_player_counts : Kafka → S3(steam-raw) parquet 저장
  3. load_player_counts    : fact_player_counts 적재, top 100 진입 게임 고정 등록

저장 경로: players/dt=YYYY-MM-DD/hour=HH/
"""

from datetime import datetime, timedelta

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.kafka_topic_to_s3 import KafkaTopicToS3Operator
from operators.silver_players_to_db import SilverPlayerCountsOperator
from operators.steam_players_to_kafka import SteamPlayerCountsToKafkaOperator

_TOPIC = "steam.players"
_SLOT = "data_interval_end"
_SNAPSHOT_PREFIX = (
    "players/dt={{ " + _SLOT + ".strftime('%Y-%m-%d') }}/hour={{ " + _SLOT + ".strftime('%H') }}"
)

with DAG(
    dag_id="steam_players_01_hourly",
    description="top 100 + 관리 목록 동접자 → Kafka → S3 → fact_player_counts (매시)",
    schedule="0 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "players"],
    default_args={
        "on_failure_callback": slack_fail_alert,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:
    produce_player_counts = SteamPlayerCountsToKafkaOperator(
        task_id="produce_player_counts",
        kafka_topic=_TOPIC,
        snapshot_id="{{ " + _SLOT + ".strftime('%Y%m%dT%H%M%S') }}",
        snapshot_prefix=_SNAPSHOT_PREFIX,
        slot_at="{{ " + _SLOT + ".int_timestamp }}",
        rate_per_sec=5.0,
        # 재시도가 늦어져 다음 슬롯에 가까워지면 현재 값을 과거 슬롯에 기록하지 않도록 실패 처리
        max_slot_lag_sec=45 * 60,
        execution_timeout=timedelta(minutes=50),
    )

    # producer가 중간에 실패해도 이미 발행한 메시지는 저장하도록 all_done
    publish_player_counts = KafkaTopicToS3Operator(
        task_id="publish_player_counts",
        trigger_rule="all_done",
        kafka_topic=_TOPIC,
        consumer_group_id="steam-players-to-s3",
        s3_key_prefix=_SNAPSHOT_PREFIX,
    )

    load_player_counts = SilverPlayerCountsOperator(
        task_id="load_player_counts",
        snapshot_prefix=_SNAPSHOT_PREFIX,
    )

    produce_player_counts >> publish_player_counts >> load_player_counts
