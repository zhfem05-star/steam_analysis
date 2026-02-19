"""
Steam 원본 데이터 수집 DAG

Steam API에서 데이터를 수집하여 MinIO(S3) steam-raw 버킷에 저장한다.
"""

from datetime import datetime

from airflow import DAG

from operators.steam_api_to_s3 import SteamApiToS3Operator

with DAG(
    dag_id="steam_rawdata_extract",
    description="Steam API → MinIO(steam-raw) 원본 데이터 수집",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["steam", "extract"],
) as dag:

    # CS2(730) 현재 동접자 수
    fetch_cs2_players = SteamApiToS3Operator(
        task_id="fetch_cs2_players",
        method="get_current_players",
        method_params={"appid": 730},
        s3_key="players/cs2_{{ ds }}.json",
    )
