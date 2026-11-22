"""
Kafka(steam-reviews) → KafkaTopicToS3Operator → S3 저장소(steam-raw) 통합 테스트

- producer(SteamReviewsToKafkaOperator)가 실제 Kafka에 발행한 메시지를
  consumer(KafkaTopicToS3Operator)가 실제 S3 저장소에 저장하는지 검증
- Steam API와 Slack 요약만 대체하고 Kafka·SeaweedFS·Postgres는 실제 서비스 사용
- 테스트 전용 Kafka 토픽·consumer group·S3 prefix를 사용하고 종료 시 정리
- 묶음당 1건으로 제한해 여러 청크 저장과 재실행 시 중복 미발생 확인

실행 방법 (airflow-scheduler 컨테이너 안에서):
    docker exec airflow-scheduler env PYTHONPATH=/opt/airflow/plugins python -m pytest /opt/airflow/tests/integration -m integration -v
"""

import random
import uuid
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from operators.kafka_topic_to_s3 import KafkaTopicToS3Operator
from operators.steam_reviews_to_kafka import SteamReviewsToKafkaOperator

pytestmark = pytest.mark.integration


def _fake_ti() -> MagicMock:
    ti = MagicMock()
    ti.dag_id, ti.task_id, ti.log_url = "integration_test", "verify", "http://test"
    return ti


def _review_chunk_keys(s3_hook: SteamS3Hook, prefix: str, appid: int) -> list[str]:
    """테스트 appid의 Bronze 리뷰 청크 키 목록"""
    return sorted(
        key
        for key in s3_hook.list_keys(prefix=prefix, bucket=BUCKET_RAW)
        if key.rsplit("/", 1)[-1].startswith(f"{appid}_chunk_")
        and key.endswith(".parquet")
    )


def test_review_flows_from_kafka_producer_to_s3_via_consumer(
    s3_hook: SteamS3Hook, test_s3_prefix: str, test_kafka_topic: str
) -> None:
    # 실제 운영 데이터·이전 테스트 잔여 메시지와 겹치지 않게 매번 새 appid 사용
    test_appid = random.randint(100_000_000, 999_999_999)  # noqa: S311

    fake_pages = [
        (
            [
                {
                    "recommendationid": "1",
                    "timestamp_created": 1755000000,
                    "review": "good game",
                },
                {
                    "recommendationid": "2",
                    "timestamp_created": 1755000100,
                    "review": "nice",
                },
            ],
            "cursor_A",
        )
    ]
    mock_api_hook = MagicMock()
    mock_api_hook.iter_review_pages.return_value = iter(fake_pages)

    # 1) producer: Steam API만 대체, Kafka·Postgres는 실제
    with (
        patch(
            "operators.steam_reviews_to_kafka.SteamApiHook", return_value=mock_api_hook
        ),
        patch("operators.steam_reviews_to_kafka.slack_collect_summary"),
    ):
        producer = SteamReviewsToKafkaOperator(
            task_id="produce",
            kafka_topic=test_kafka_topic,
            app_ids=[test_appid],
            languages=["korean"],
        )
        producer.execute({"ti": _fake_ti()})

    # 2) consumer: 실제 Kafka를 끝 offset까지 소비해 실제 S3 저장소에 저장
    consumer = KafkaTopicToS3Operator(
        task_id="consume",
        kafka_topic=test_kafka_topic,
        consumer_group_id=f"integration-test-{uuid.uuid4().hex}",
        s3_key_prefix=test_s3_prefix,
        partition_by="appid",
        max_records_per_flush=1,
    )
    summary = consumer.execute({})

    # 3) 검증: 묶음별 청크 2개와 내용
    keys = _review_chunk_keys(s3_hook, test_s3_prefix, test_appid)
    assert summary["records"] == 2, f"메시지 2건을 소비해야 함: summary={summary}"
    assert summary["commits"] == 2, f"묶음마다 commit해야 함: summary={summary}"
    assert len(keys) == 2, f"묶음별 청크 2개가 필요함: keys={keys}"

    df = pl.concat([s3_hook.read_parquet(key=key, bucket=BUCKET_RAW) for key in keys])
    assert df.shape[0] == 2
    assert set(df["recommendationid"]) == {"1", "2"}
    assert set(df["appid"]) == {test_appid}
    assert set(df["language"]) == {"korean"}

    # 4) 같은 group으로 재실행: commit된 offset 이후 메시지가 없으므로 추가 저장 없음
    rerun = consumer.execute({})
    assert rerun["records"] == 0, (
        f"재실행 시 이미 저장한 메시지를 다시 읽으면 안 됨: {rerun}"
    )
    assert _review_chunk_keys(s3_hook, test_s3_prefix, test_appid) == keys
