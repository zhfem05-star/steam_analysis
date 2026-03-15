"""
Steam 리뷰 원본 데이터 수집 DAG

steam_rawdata_extract DAG의 push_app_ids 태스크가 완료되면
ExternalTaskSensor로 감지한 뒤, 리뷰를 수집해 MinIO(S3) steam-raw 버킷에 저장한다.

저장 경로: reviews/{YYYYMMDD_HHMM}/{appid}_reviews.json
"""

from datetime import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from operators.steam_reviews_to_s3 import SteamReviewsToS3Operator


def _get_rawdata_execution_date(execution_date, **_):
    """
    테스트를 위해 수동 실행을 위한 설정
    수동 실행 시에도 당일 01:00 UTC 기준의 steam_rawdata_extract 실행을 참조.
    01:00 이전 실행은 전날 01:00을 참조.
    """
    target = execution_date.replace(hour=1, minute=0, second=0, microsecond=0)
    if execution_date.hour < 1:
        target = target.subtract(days=1)
    return target


with DAG(
    dag_id="steam_review_extract",
    description="Steam API → MinIO(steam-raw) 게임 리뷰 원본 데이터 수집",
    schedule="0 1 * * *",      # steam_rawdata_extract 와 동일 스케줄
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "extract"],
) as dag:

    # steam_rawdata_extract DAG의 push_app_ids 완료를 감지
    wait_for_app_ids = ExternalTaskSensor(
        task_id="wait_for_app_ids",
        external_dag_id="steam_rawdata_extract",
        external_task_id="push_app_ids",
        execution_date_fn=_get_rawdata_execution_date,
        mode="reschedule",      # slot을 점유하지 않고 주기적으로 재스케줄
        poke_interval=60,       # 60초마다 확인
        timeout=3600,           # 최대 1시간 대기
    )

    # XCom에서 appid 리스트를 받아 리뷰 수집 후 S3 저장
    collect_reviews = SteamReviewsToS3Operator(
        task_id="collect_reviews",
        app_ids_dag_id="steam_rawdata_extract",
        app_ids_task_id="push_app_ids",
        s3_key_prefix="reviews/{{ execution_date.strftime('%Y%m%d_%H%M') }}",
    )

    wait_for_app_ids >> collect_reviews