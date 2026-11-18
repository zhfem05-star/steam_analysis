"""
Steam 리뷰 원본 데이터 수집 DAG

tracked_games 컨트롤 테이블에서 app_id를 조회하여 한국어·영어 리뷰를 전체 수집한다.

수집 구조:
  - collect_reviews (producer) : Steam API → Kafka 토픽(steam-reviews) 발행
  - Spark consumer 제거 후 Kafka → S3 consumer는 재구성 2단계에서 추가
    (docs/pipeline_plan.md). 그전까지 메시지는 Kafka 토픽에 보관
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.steam_reviews_to_kafka import SteamReviewsToKafkaOperator


with DAG(
    dag_id="steam_bronze_02_review_extract",
    description="Steam API → Kafka 게임 리뷰 원본 데이터 발행 (한국어·영어)",
    schedule="0 8 * * *",      # 매일 08:00 UTC (한국시간 오후 5시)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "extract"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    collect_reviews = SteamReviewsToKafkaOperator(
        task_id="collect_reviews",
        kafka_topic="steam-reviews",
        collection_mode="continuous",
        filter_type="recent",
        date_range_type="include",
        start_date=946684800,  # 2000-01-01 UTC. 시작 시각 0은 Steam 날짜 필터를 무시할 수 있어 사용하지 않는다.
        window_days=30,
        incremental_overlap_seconds=86400,
        # app_ids=[730],  # 730, 570, 1623730 CS2, Dota2, palworld. 테스트용으로 소수 지정. 전체 수집 시 UpsertTrackedGamesOperator에서 app_id 리스트를 XCom으로 받아 사용.
        # app_id_limit : 수집할 게임 수 제한. 테스트 시 소수 지정, 전체 수집 시 None
        # app_id_limit=5,
        # languages    : 수집 언어 목록. 기본값 ["korean", "english"]
        # filter_type  : "recent"(최신순) | "all"(Steam 추천순) | "updated"(최근 수정순). 기본값 "recent"
    )
