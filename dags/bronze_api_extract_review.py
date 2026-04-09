"""
Steam 리뷰 원본 데이터 수집 DAG

tracked_games 컨트롤 테이블에서 app_id를 조회하여
한국어·영어 리뷰를 전체 수집하고 MinIO(S3) steam-raw 버킷에 저장한다.

저장 경로: reviews/{YYYYMMDD_HHMM}/{appid}_reviews.json
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.steam_reviews_to_s3 import SteamReviewsToS3Operator


with DAG(
    dag_id="steam_bronze_02_review_extract",
    description="Steam API → MinIO(steam-raw) 게임 리뷰 원본 데이터 수집 (한국어·영어)",
    schedule="0 8 * * *",      # 매일 08:00 UTC (한국시간 오후 5시)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "extract"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    collect_reviews = SteamReviewsToS3Operator(
        task_id="collect_reviews",
        s3_key_prefix="reviews/{{ execution_date.strftime('%Y%m%d_%H%M') }}",
        # app_ids=[730],  # 730, 570, 1623730 CS2, Dota2, palworld. 테스트용으로 소수 지정. 전체 수집 시 UpsertTrackedGamesOperator에서 app_id 리스트를 XCom으로 받아 사용.
        # app_id_limit : 수집할 게임 수 제한. 테스트 시 소수 지정, 전체 수집 시 None
        # app_id_limit=5,
        # languages    : 수집 언어 목록. 기본값 ["korean", "english"]
        # num_reviews  : 언어별 최대 리뷰 수. 기본값 None (전체 수집)
        # filter_type  : "recent"(최신순) | "all"(Steam 추천순) | "updated"(최근 수정순). 기본값 "recent"
    )
