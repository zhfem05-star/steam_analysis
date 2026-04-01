"""
Steam 원본 데이터 수집 DAG
할인된 게임의 APP_ID를 먼저 수집하여 이후 수집할 게임 데이터 범위를 지정
Steam API에서 데이터를 수집하여 MinIO(S3) steam-raw 버킷에 저장한다.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks.slack_callback import slack_fail_alert
from hooks.s3_hook import SteamS3Hook
from operators.steam_api_to_s3 import SteamApiToS3Operator
from operators.upsert_tracked_games import UpsertTrackedGamesOperator


def _extract_app_ids(**context):
    """S3에 저장된 할인 게임 JSON에서 appid 리스트를 추출해 XCom으로 반환."""
    s3_key = context["ti"].xcom_pull(task_ids="fetch_discount_games")
    s3_hook = SteamS3Hook()
    data = s3_hook.read_json(key=s3_key)
    app_ids = [item["appid"] for item in data["response"]["ids"]]
    return app_ids  # XCom return_value 로 자동 push


with DAG(
    dag_id="steam_bronze_01_discount_game_extract",
    description="Steam API → MinIO(steam-raw) 할인 게임 원본 데이터 수집",
    schedule="0 1 * * *",      # 매일 새벽 1시 UTC (한국시간 오전 10시)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "extract"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    # 할인 중인 게임 목록 조회 → steam-raw 버킷에 저장
    fetch_discount_games = SteamApiToS3Operator(
        task_id="fetch_discount_games",
        method="get_query",
        method_params={
            "sort": 12,             # TOP_SELLERS
            "count": 400,
            "filters": {
                "released_only": True,
                "type_filters": {                       # type_filters (복수)
                    "include_games": True,
                },
                "price_filters": {
                    "exclude_free_items": True,         # 무료 게임 제외
                    "min_discount_percent": 1,          # 1% 이상 할인 중인 게임만
                },
            },
            "context": {
                "language": "korean",
                "country_code": "KR",
            },
            "data_request": {
                "include_basic_info": True,             # 이름, 설명 등
                "include_all_purchase_options": True,   # 가격, 할인 정보
            },
        },
        s3_key="discount_games/{{ execution_date.strftime('%Y%m%d_%H%M') }}_discount_game_data.json",
    )

    # S3 JSON → appid 리스트 추출 후 XCom push (downstream DAG에서 참조)
    push_app_ids = PythonOperator(
        task_id="push_app_ids",
        python_callable=_extract_app_ids,
    )

    # appid 리스트 → tracked_games 테이블 UPSERT (동접자 추적 목록 누적 관리)
    upsert_tracked_games = UpsertTrackedGamesOperator(
        task_id="upsert_tracked_games",
        app_ids_task_id="push_app_ids",
    )

    fetch_discount_games >> push_app_ids >> upsert_tracked_games
