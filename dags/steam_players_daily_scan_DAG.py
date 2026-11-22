"""
Steam 동접자 일별 스캔·관리 목록 재배분 DAG

전체 게임을 매일 조회하지 않고 스캔 일정(next_scan_at)이 도래한 비활성 게임만 조회한다.
저조한 게임은 스캔 간격을 1일 → 2일 → 4일 … 최대 30일로 늘린다.

처리 순서:
  1. sync_watchlist  : dim_apps 신규 게임 등록, 신규 게재·할인 시작·리뷰 급증 게임 우선 스캔 표시
  2. produce_scan    : 일정 도래 게임 개별 호출 → Kafka(steam.player_scan), manifest 기록
  3. publish_scan    : Kafka → S3(steam-raw) parquet 저장
  4. apply_scan      : 스캔 결과로 다음 스캔 일정 갱신
  5. rebalance       : 호출 예산 N으로 매시 수집 대상 재배분

Airflow Variable (없으면 기본값):
  player_watchlist_budget   예산 대상 게임 수 (기본 2000)
  player_scan_max_per_run   1회 최대 스캔 수 (기본 30000, 초당 5건 기준 약 100분)
  player_scan_low_threshold 저조 판단 동접자 수 (기본 10)

저장 경로: player_scan/{ts_nodash}/
"""

from datetime import datetime, timedelta

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.kafka_topic_to_s3 import KafkaTopicToS3Operator
from operators.player_watchlist_maintenance import (
    RebalancePlayerWatchlistOperator,
    SyncPlayerWatchlistOperator,
)
from operators.silver_players_to_db import SilverPlayerScanOperator
from operators.steam_players_to_kafka import SteamPlayerScanToKafkaOperator

_TOPIC = "steam.player_scan"
_SNAPSHOT_PREFIX = "player_scan/{{ ts_nodash }}"

with DAG(
    dag_id="steam_players_02_daily_scan",
    description="동접자 스캔 → Kafka → S3 → player_watchlist 일정·예산 갱신 (일별)",
    # 가격 수집(18:30 UTC) 이후라 할인 시작·리뷰 급증 우선 스캔에 당일 변화가 반영됨
    schedule="0 21 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "players"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:
    sync_watchlist = SyncPlayerWatchlistOperator(task_id="sync_watchlist")

    produce_scan = SteamPlayerScanToKafkaOperator(
        task_id="produce_scan",
        kafka_topic=_TOPIC,
        snapshot_id="{{ ts_nodash }}",
        snapshot_prefix=_SNAPSHOT_PREFIX,
        max_targets="{{ var.value.get('player_scan_max_per_run', '30000') }}",
        rate_per_sec=5.0,
        execution_timeout=timedelta(hours=3),
    )

    # producer가 중간에 실패해도 이미 발행한 메시지는 저장하도록 all_done
    publish_scan = KafkaTopicToS3Operator(
        task_id="publish_scan",
        trigger_rule="all_done",
        kafka_topic=_TOPIC,
        consumer_group_id="steam-player-scan-to-s3",
        s3_key_prefix=_SNAPSHOT_PREFIX,
    )

    apply_scan = SilverPlayerScanOperator(
        task_id="apply_scan",
        snapshot_prefix=_SNAPSHOT_PREFIX,
        low_threshold="{{ var.value.get('player_scan_low_threshold', '10') }}",
        max_interval_days=30,
    )

    rebalance = RebalancePlayerWatchlistOperator(
        task_id="rebalance",
        budget="{{ var.value.get('player_watchlist_budget', '2000') }}",
    )

    sync_watchlist >> produce_scan >> publish_scan >> apply_scan >> rebalance
