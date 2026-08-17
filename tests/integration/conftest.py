"""
Integration test 전용 conftest.

이 폴더의 테스트는 mock 없이 실제 MinIO/Kafka/Spark/Postgres에 접속한다.
unit test(tests/conftest.py)와 달리, airflow-scheduler 컨테이너 안에서
(docker exec) 실행하는 것을 전제로 한다 — 그래야 실제 DAG와 동일한
AIRFLOW_CONN_* 환경변수와 컨테이너 내부 호스트명(kafka:9092,
spark-master:7077, minio:9000 등)을 그대로 쓸 수 있다.

실행 방법:
    docker exec airflow-scheduler pytest /opt/airflow/tests/integration -m integration -v
"""

import uuid

import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook


@pytest.fixture(scope="session", autouse=True)
def _require_services():
    """MinIO/Postgres에 실제로 연결 안 되면 전체 integration 세션을 skip."""
    try:
        SteamS3Hook(aws_conn_id="minio_s3").list_keys(prefix="", bucket=BUCKET_RAW)
    except Exception as e:
        pytest.skip(f"MinIO에 연결할 수 없습니다: {e}")

    try:
        PostgresHook(postgres_conn_id="analytics_db").get_conn()
    except Exception as e:
        pytest.skip(f"analytics-postgres에 연결할 수 없습니다: {e}")


@pytest.fixture
def s3_hook():
    """실제 S3Hook (mock 아님) — integration test는 진짜로 MinIO에 붙습니다."""
    return SteamS3Hook(aws_conn_id="minio_s3")


@pytest.fixture
def pg_hook():
    return PostgresHook(postgres_conn_id="analytics_db")


@pytest.fixture
def test_s3_prefix(s3_hook):
    """테스트마다 고유 prefix를 써서 실 데이터와 안 섞이게 하고, 끝나면 정리."""
    prefix = f"integration-test/{uuid.uuid4().hex}"
    yield prefix
    keys = s3_hook.list_keys(prefix=prefix, bucket=BUCKET_RAW)
    if keys:
        s3_hook._hook.delete_objects(bucket=BUCKET_RAW, keys=keys)
