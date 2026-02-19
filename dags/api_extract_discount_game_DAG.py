"""
Steam 원본 데이터 수집 DAG
할인된 게임의 APP_ID를 먼저 수집하여 이후 수집할 게임 데이터 범위를 지정
Steam API에서 데이터를 수집하여 MinIO(S3) steam-raw 버킷에 저장한다.
"""

from datetime import datetime

from airflow import DAG

from operators.steam_api_to_s3 import SteamApiToS3Operator

with DAG(
    dag_id="steam_rawdata_extract",
    description="Steam API → MinIO(steam-raw) 할인 게임 원본 데이터 수집",
    schedule="0 1 * * *",      # 매일 새벽 1시 UTC (한국시간 오전 10시)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "extract"],
) as dag:

    # 할인 중인 게임 목록 조회 → steam-raw 버킷에 저장
    fetch_discount_games = SteamApiToS3Operator(
        task_id="fetch_discount_games",
        method="get_query",
        method_params={
            "sort": 12,             # TOP_SELLERS
            "count": 1000,
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
