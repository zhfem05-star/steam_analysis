"""
Steam 앱 상세 정보 수집 DAG

tracked_games 컨트롤 테이블에서 app_id를 조회하여
appdetails API에서 게임 메타데이터(이름·장르·플랫폼·개발사 등)를 수집하고
MinIO(S3) steam-raw 버킷에 Parquet 형태로 저장한다.

저장 경로: appdetails/{YYYYMMDD_HHMM}/{appid}.parquet
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.steam_app_details_to_s3 import SteamAppDetailsToS3Operator


with DAG(
    dag_id="steam_bronze_04_app_details_extract",
    description="Steam appdetails API → MinIO(steam-raw) 게임 메타데이터 수집 (장르·플랫폼·개발사 등)",
    schedule="0 9 * * *",      # 매일 09:00 UTC (01번 DAG 완료 후)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "extract"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    collect_app_details = SteamAppDetailsToS3Operator(
        task_id="collect_app_details",
        s3_key_prefix="appdetails/{{ execution_date.strftime('%Y%m%d_%H%M') }}",
        # app_id_limit=5,   # 테스트 시 소수 지정, 전체 수집 시 None
        # cc="kr",          # 국가 코드 기본값 "kr"
        # language="korean" # 언어 기본값 "korean"
    )
