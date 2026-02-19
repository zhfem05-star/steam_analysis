"""
MinIO 튜토리얼 DAG
- steam-raw 버킷에 hello-minio.json 파일을 업로드하는 테스트 DAG
"""

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_hello_minio():
    """MinIO(steam-raw 버킷)에 테스트 JSON 파일을 업로드"""
    hook = S3Hook(aws_conn_id="minio_s3")

    data = {
        "message": "Hello MinIO!",
        "project": "steam-project",
        "timestamp": datetime.now().isoformat(),
    }

    hook.load_string(
        string_data=json.dumps(data, ensure_ascii=False, indent=2),
        key="hello-minio.json",
        bucket_name="steam-raw",
        replace=True,
    )
    print("hello-minio.json 업로드 완료!")


with DAG(
    dag_id="minio_tutorial",
    description="MinIO 연결 테스트 - steam-raw에 hello-minio.json 업로드",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tutorial", "minio"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_hello_minio",
        python_callable=upload_hello_minio,
    )
