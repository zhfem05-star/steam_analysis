"""
S3 저장소 연결 확인 DAG

- Airflow Connection `s3_storage`로 steam-raw 버킷에 테스트 JSON 업로드
- 로컬 SeaweedFS와 운영 AWS S3 모두 같은 방식으로 확인
"""

import json
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

_SMOKE_TEST_KEY = "_smoke_test/hello-s3.json"


def upload_smoke_test_object() -> None:
    """steam-raw 버킷에 테스트 JSON 업로드"""
    hook = S3Hook(aws_conn_id="s3_storage")

    data = {
        "message": "S3 storage connection OK",
        "project": "steam-project",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    hook.load_string(
        string_data=json.dumps(data, ensure_ascii=False, indent=2),
        key=_SMOKE_TEST_KEY,
        bucket_name="steam-raw",
        replace=True,
    )
    print(f"{_SMOKE_TEST_KEY} 업로드 완료")


with DAG(
    dag_id="s3_storage_smoke_test",
    description="S3 저장소 연결 확인 - steam-raw에 테스트 JSON 업로드",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tutorial", "s3"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_smoke_test_object",
        python_callable=upload_smoke_test_object,
    )
