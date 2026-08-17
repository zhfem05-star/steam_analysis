"""
Kafka(steam-reviews) → Spark → MinIO(steam-raw) end-to-end integration test.

producer(SteamReviewsToKafkaOperator)가 실제 Kafka에 발행한 메시지를
consumer(SparkReviewsKafkaToS3Operator)가 실제 Spark 클러스터로 소비해서
MinIO에 정확히 저장하는지, 전체 경로를 mock 없이 검증한다.

Steam API 호출만 mock하고(고정된 리뷰 2건), Kafka·Spark·MinIO·Postgres는
전부 실제 서비스를 사용한다.

주의: steam-reviews 토픽은 실제 운영 DAG도 같이 쓰는 공유 토픽이고,
Spark 쪽에서 startingOffsets=earliest로 전체 이력을 다시 읽는 구조라
- appid는 매 실행마다 랜덤하게 생성해서 이전 테스트/운영 데이터와 절대
  충돌하지 않게 한다 (겹치면 assert 카운트가 어긋남).
- 토픽이 계속 쌓일수록 매 실행마다 전체 이력을 다시 스캔하므로 실행 시간이
  점점 길어질 수 있다 (이 프로젝트 규모에서는 감내 가능한 수준으로 판단).

실행 방법 (airflow-scheduler 컨테이너 안에서, pyspark/JDK/kafka-python 필요):
    docker exec airflow-scheduler pytest /opt/airflow/tests/integration -m integration -v
"""

import random
import shutil
from unittest.mock import MagicMock, patch

import pytest

from hooks.s3_hook import BUCKET_RAW
from operators.spark_reviews_kafka_to_s3 import SparkReviewsKafkaToS3Operator
from operators.steam_reviews_to_kafka import SteamReviewsToKafkaOperator

pytestmark = pytest.mark.integration


def _fake_ti():
    ti = MagicMock()
    ti.dag_id, ti.task_id, ti.log_url = "integration_test", "verify", "http://test"
    return ti


def test_review_flows_from_kafka_producer_to_s3_via_spark(s3_hook, test_s3_prefix):
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
            kafka_topic="steam-reviews",
            app_ids=[test_appid],
            languages=["korean"],
        )
        producer.execute({"ti": _fake_ti()})

    # 2) consumer: 실제 Spark 클러스터가 실제 Kafka를 소비해서 MinIO에 저장
    checkpoint_path = f"/opt/spark/checkpoints/{test_s3_prefix.split('/')[-1]}"
    consumer = SparkReviewsKafkaToS3Operator(
        task_id="consume",
        kafka_topic="steam-reviews",
        s3_key_prefix=test_s3_prefix,
        checkpoint_path=checkpoint_path,
    )
    try:
        consumer.execute({})

        # 3) 검증: 실제로 MinIO에 저장됐는지 + 내용이 정확한지
        key = f"{test_s3_prefix}/{test_appid}_chunk_000.parquet"
        assert s3_hook.key_exists(key=key, bucket=BUCKET_RAW)

        df = s3_hook.read_parquet(key=key, bucket=BUCKET_RAW)
        assert df.shape[0] == 2
        assert set(df["recommendationid"]) == {"1", "2"}
        assert df["appid"][0] == test_appid
        assert df["language"][0] == "korean"
    finally:
        shutil.rmtree(checkpoint_path, ignore_errors=True)
