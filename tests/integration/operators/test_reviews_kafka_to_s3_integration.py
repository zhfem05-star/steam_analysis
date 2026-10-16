"""
Kafka(steam-reviews) → Spark → MinIO(steam-raw) end-to-end integration test.

producer(SteamReviewsToKafkaOperator)가 실제 Kafka에 발행한 메시지를
consumer(SparkReviewsKafkaToS3Operator)가 실제 Spark 클러스터로 소비해서
MinIO에 정확히 저장하는지, 전체 경로를 mock 없이 검증한다.

Steam API 호출만 mock하고(고정된 리뷰 2건), Kafka·Spark·MinIO·Postgres는
전부 실제 서비스를 사용한다.

테스트 전용 Kafka 토픽·S3 prefix·Spark 체크포인트를 사용하고 종료 시 정리
배치당 1개 메시지를 읽어 여러 청크에 걸친 저장 결과 검증

실행 방법 (airflow-scheduler 컨테이너 안에서, pyspark/JDK/kafka-python 필요):
    docker exec airflow-scheduler env PYTHONPATH=/opt/airflow/plugins python -m pytest /opt/airflow/tests/integration -m integration -v
"""

import random
import shutil
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from operators.spark_reviews_kafka_to_s3 import SparkReviewsKafkaToS3Operator
from operators.steam_reviews_to_kafka import SteamReviewsToKafkaOperator

pytestmark = pytest.mark.integration


def _fake_ti():
    ti = MagicMock()
    ti.dag_id, ti.task_id, ti.log_url = "integration_test", "verify", "http://test"
    return ti


def test_review_flows_from_kafka_producer_to_s3_via_spark(
    s3_hook: SteamS3Hook, test_s3_prefix: str, test_kafka_topic: str
) -> None:
    # 실제 운영 데이터·이전 테스트 잔여 메시지와 절대 겹치지 않게 매번 새 appid 사용
    test_appid = random.randint(100_000_000, 999_999_999)

    fake_pages = [
        ([
            {"recommendationid": "1", "timestamp_created": 1755000000, "review": "good game"},
            {"recommendationid": "2", "timestamp_created": 1755000100, "review": "nice"},
        ], "cursor_A")
    ]
    mock_api_hook = MagicMock()
    mock_api_hook.iter_review_pages.return_value = iter(fake_pages)

    # 1) producer: Steam API만 mock, Kafka·Postgres는 실제
    with patch("operators.steam_reviews_to_kafka.SteamApiHook", return_value=mock_api_hook), \
         patch("operators.steam_reviews_to_kafka.slack_collect_summary"):
        producer = SteamReviewsToKafkaOperator(
            task_id="produce",
            kafka_topic=test_kafka_topic,
            app_ids=[test_appid],
            languages=["korean"],
        )
        producer.execute({"ti": _fake_ti()})

    # 2) consumer: 실제 Spark 클러스터가 실제 Kafka를 소비해서 MinIO에 저장
    checkpoint_path = f"/opt/spark/checkpoints/{test_s3_prefix.split('/')[-1]}"
    consumer = SparkReviewsKafkaToS3Operator(
        task_id="consume",
        kafka_topic=test_kafka_topic,
        s3_key_prefix=test_s3_prefix,
        checkpoint_path=checkpoint_path,
        max_offsets_per_trigger=1,
    )
    try:
        consumer.execute({})

        # 3) 검증: 실제로 MinIO에 저장됐는지 + 내용이 정확한지
        keys = sorted(
            key
            for key in s3_hook.list_keys(prefix=test_s3_prefix, bucket=BUCKET_RAW)
            if key.rsplit("/", 1)[-1].startswith(f"{test_appid}_chunk_")
            and key.endswith(".parquet")
        )
        assert len(keys) == 2, f"배치별 청크 2개가 필요합니다: keys={keys}"
        df = pl.concat(
            [s3_hook.read_parquet(key=key, bucket=BUCKET_RAW) for key in keys]
        )
        assert df.shape[0] == 2
        assert set(df["recommendationid"]) == {"1", "2"}
        assert df["appid"][0] == test_appid
        assert df["language"][0] == "korean"
    finally:
        shutil.rmtree(checkpoint_path, ignore_errors=True)
