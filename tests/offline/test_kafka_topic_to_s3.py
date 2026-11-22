"""
KafkaTopicToS3Operator 오프라인 검증

- 실제 Kafka 대신 끝 offset·poll·commit을 흉내 내는 FakeConsumer 사용
- S3 업로드와 offset commit의 순서, 끝 offset 경계, 실패 시 commit 보류 확인
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl
import pytest
from kafka import TopicPartition

from operators.kafka_topic_to_s3 import BufferedRecord, KafkaTopicToS3Operator

TOPIC = "test-topic"

# 파티션 번호별 메시지 본문 목록
PartitionMessages = dict[int, list[dict[str, Any]]]


@dataclass(frozen=True)
class FakeRecord:
    """poll 결과의 ConsumerRecord 대체"""

    offset: int
    value: dict[str, Any]


class FakeConsumer:
    """
    파티션별 메시지 목록을 끝 offset까지 순서대로 돌려주는 consumer 대체

    - commit 시 현재 위치를 committed에 기록하고 호출 순서를 events에 남김
    - poll 1회에 파티션별 batch_size건까지 반환하되 실제 KafkaConsumer처럼
      전체 반환 수는 max_records로 제한
    """

    def __init__(
        self,
        messages: PartitionMessages,
        events: list[str],
        batch_size: int = 2,
        start_offsets: dict[int, int] | None = None,
    ) -> None:
        self.messages = messages
        self.events = events
        self.batch_size = batch_size
        self.positions = {p: (start_offsets or {}).get(p, 0) for p in messages}
        self.committed: dict[int, int] = {}
        self.assigned: list[TopicPartition] = []
        self.empty_polls_remaining = 0
        self.poll_limits: list[int | None] = []

    def partitions_for_topic(self, topic: str) -> set[int]:
        return set(self.messages) if topic == TOPIC else set()

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned = partitions

    def end_offsets(
        self, partitions: list[TopicPartition]
    ) -> dict[TopicPartition, int]:
        return {tp: len(self.messages[tp.partition]) for tp in partitions}

    def position(self, partition: TopicPartition) -> int:
        return self.positions[partition.partition]

    def poll(
        self,
        timeout_ms: int,  # noqa: ARG002
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[FakeRecord]]:
        if self.empty_polls_remaining:
            self.empty_polls_remaining -= 1
            return {}

        self.poll_limits.append(max_records)
        remaining = max_records if max_records is not None else float("inf")

        result: dict[TopicPartition, list[FakeRecord]] = {}
        for partition, values in self.messages.items():
            start = self.positions[partition]
            end = int(min(start + self.batch_size, len(values), start + remaining))
            if start < end:
                remaining -= end - start
                result[TopicPartition(TOPIC, partition)] = [
                    FakeRecord(offset, values[offset]) for offset in range(start, end)
                ]
                self.positions[partition] = end
        return result

    def commit(self) -> None:
        self.committed = dict(self.positions)
        self.events.append(f"commit:{sorted(self.committed.items())}")


class FakeS3Hook:
    """업로드한 DataFrame을 키별로 보관하고 호출 순서를 events에 남기는 S3 Hook 대체"""

    def __init__(
        self, events: list[str], fail_on: Callable[[str], bool] | None = None
    ) -> None:
        self.events = events
        self.fail_on = fail_on
        self.objects: dict[str, pl.DataFrame] = {}

    def upload_parquet(self, df: pl.DataFrame, key: str, bucket: str) -> None:  # noqa: ARG002
        if self.fail_on is not None and self.fail_on(key):
            raise OSError(f"upload failed: {key}")
        self.objects[key] = df
        self.events.append(f"upload:{key}")


def make_operator(**overrides: Any) -> KafkaTopicToS3Operator:
    """테스트 기본 설정의 operator 생성"""
    options: dict[str, Any] = {
        "task_id": "consume",
        "kafka_topic": TOPIC,
        "consumer_group_id": "test-group",
        "s3_key_prefix": "reviews/run",
        "partition_by": "appid",
        "max_records_per_flush": 100,
        "max_idle_polls": 3,
    }
    options.update(overrides)
    return KafkaTopicToS3Operator(**options)


def review(appid: int, review_id: str) -> dict[str, Any]:
    """appid와 리뷰 ID만 가진 최소 리뷰 메시지"""
    return {
        "appid": appid,
        "recommendationid": review_id,
        "review": f"text {review_id}",
    }


# ── 정상 소비 ──────────────────────────────────────


def test_drains_until_end_and_groups_files_by_appid() -> None:
    events: list[str] = []
    messages = {
        0: [review(1, "a"), review(2, "b"), review(1, "c")],
        1: [review(2, "d")],
    }
    consumer, s3 = FakeConsumer(messages, events), FakeS3Hook(events)

    summary = make_operator().drain(consumer, s3)

    assert summary == {"topic": TOPIC, "records": 4, "files": 2, "commits": 1}
    assert consumer.committed == {0: 3, 1: 1}, "끝 offset까지 commit 되어야 함"

    stored = {
        key.rsplit("/", 1)[-1].split("_chunk_")[0]: df for key, df in s3.objects.items()
    }
    assert set(stored) == {"1", "2"}, f"appid별 파일이 필요함: keys={list(s3.objects)}"
    assert set(stored["1"]["recommendationid"]) == {"a", "c"}
    assert set(stored["2"]["recommendationid"]) == {"b", "d"}
    assert all(key.startswith("reviews/run/") for key in s3.objects)


def test_commit_happens_only_after_every_upload_of_the_batch() -> None:
    events: list[str] = []
    messages = {0: [review(1, "a"), review(2, "b"), review(3, "c"), review(1, "d")]}
    consumer, s3 = FakeConsumer(messages, events), FakeS3Hook(events)

    make_operator(max_records_per_flush=2).drain(consumer, s3)

    # 묶음 2개: 각 묶음의 업로드가 모두 끝난 뒤 commit
    kinds = [event.split(":", 1)[0] for event in events]
    assert kinds == ["upload", "upload", "commit", "upload", "upload", "commit"], events
    assert consumer.committed == {0: 4}


def test_batch_never_exceeds_limit_even_when_poll_could_return_more() -> None:
    events: list[str] = []
    messages = {
        0: [review(1, "a"), review(1, "b"), review(1, "c")],
        1: [review(1, "d"), review(1, "e")],
    }
    # 실제 poll은 기본 500건까지 한 번에 반환할 수 있음
    consumer = FakeConsumer(messages, events, batch_size=500)
    s3 = FakeS3Hook(events)

    summary = make_operator(max_records_per_flush=2).drain(consumer, s3)

    sizes = sorted(df.height for df in s3.objects.values())
    assert sizes == [1, 2, 2], f"묶음은 최대 2건이어야 함: sizes={sizes}"
    assert summary == {"topic": TOPIC, "records": 5, "files": 3, "commits": 3}
    assert all(limit is not None and limit <= 2 for limit in consumer.poll_limits), (
        f"poll은 남은 묶음 용량만 요청해야 함: {consumer.poll_limits}"
    )


def test_resumes_from_committed_offset_without_overwriting_previous_files() -> None:
    events: list[str] = []
    values = [review(1, str(i)) for i in range(4)]

    first_s3 = FakeS3Hook(events)
    make_operator(max_records_per_flush=2).drain(
        FakeConsumer({0: values[:2]}, events), first_s3
    )

    # 앞의 2건이 commit된 뒤 같은 토픽에 2건이 추가된 상황
    second_s3 = FakeS3Hook(events)
    make_operator(max_records_per_flush=2).drain(
        FakeConsumer({0: values}, events, start_offsets={0: 2}), second_s3
    )

    assert set(first_s3.objects).isdisjoint(second_s3.objects), (
        "다른 묶음은 다른 파일명이어야 함"
    )
    second_ids = pl.concat(list(second_s3.objects.values()))[
        "recommendationid"
    ].to_list()
    assert sorted(second_ids) == ["2", "3"], "commit 이후 메시지만 다시 읽어야 함"


def test_stops_at_start_snapshot_and_leaves_later_messages() -> None:
    events: list[str] = []
    consumer = FakeConsumer(
        {0: [review(1, "a"), review(1, "b"), review(1, "late")]}, events
    )
    s3 = FakeS3Hook(events)

    # 시작 시점 끝 offset을 2로 고정: offset 2는 실행 도중 들어온 메시지
    consumer.end_offsets = lambda partitions: {tp: 2 for tp in partitions}  # type: ignore[method-assign]
    summary = make_operator().drain(consumer, s3)

    assert summary["records"] == 2, "시작 시점 끝 offset까지만 읽어야 함"
    assert consumer.committed == {0: 2}, (
        "이후 메시지는 다음 실행에서 읽도록 commit 위치 유지"
    )


def test_empty_or_missing_topic_skips_without_upload_or_commit() -> None:
    events: list[str] = []
    s3 = FakeS3Hook(events)

    empty = make_operator().drain(FakeConsumer({0: []}, events), s3)
    missing = make_operator(kafka_topic="other-topic").drain(
        FakeConsumer({0: []}, events), s3
    )

    assert empty["records"] == 0
    assert missing == {"topic": "other-topic", "records": 0, "files": 0, "commits": 0}
    assert events == [], f"업로드나 commit이 없어야 함: {events}"


# ── 실패 처리 ──────────────────────────────────────


def test_upload_failure_keeps_offsets_uncommitted() -> None:
    events: list[str] = []
    consumer = FakeConsumer({0: [review(1, "a"), review(2, "b")]}, events)
    s3 = FakeS3Hook(events, fail_on=lambda key: "/2_chunk_" in key)

    with pytest.raises(OSError, match="upload failed"):
        make_operator().drain(consumer, s3)

    assert consumer.committed == {}, "업로드 실패 시 commit하면 안 됨"
    assert not any(event.startswith("commit") for event in events)


def test_stalled_consumer_fails_instead_of_reporting_partial_success() -> None:
    events: list[str] = []
    consumer = FakeConsumer({0: [review(1, "a")]}, events)
    consumer.empty_polls_remaining = 10

    with pytest.raises(RuntimeError, match="끝 offset 도달 전 메시지 수신 중단"):
        make_operator(max_idle_polls=3).drain(consumer, FakeS3Hook(events))

    assert consumer.committed == {}


def test_message_without_partition_field_is_rejected() -> None:
    buffer = [BufferedRecord(0, 5, {"recommendationid": "a"})]

    with pytest.raises(ValueError, match="field=appid, partition=0, offset=5"):
        KafkaTopicToS3Operator.group_records(buffer, "appid")


# ── 파일명·설정 ──────────────────────────────────────


def test_batch_token_depends_only_on_partition_start_offsets() -> None:
    token = KafkaTopicToS3Operator.batch_token
    first = [
        BufferedRecord(0, 10, {}),
        BufferedRecord(1, 3, {}),
        BufferedRecord(0, 11, {}),
    ]
    reordered = [
        BufferedRecord(0, 11, {}),
        BufferedRecord(1, 3, {}),
        BufferedRecord(0, 10, {}),
    ]
    later = [BufferedRecord(0, 12, {}), BufferedRecord(1, 3, {})]

    assert token(first) == token(reordered), (
        "poll 순서가 달라도 같은 묶음이면 같은 토큰"
    )
    assert token(first) != token(later), "시작 offset이 다르면 다른 토큰"


def test_without_partition_field_writes_single_file_per_batch() -> None:
    events: list[str] = []
    s3 = FakeS3Hook(events)

    make_operator(partition_by=None).drain(
        FakeConsumer({0: [review(1, "a"), review(2, "b")]}, events), s3
    )

    assert len(s3.objects) == 1
    assert next(iter(s3.objects)).split("/")[-1].startswith("chunk_")


@pytest.mark.parametrize(
    "field", ["max_records_per_flush", "poll_timeout_ms", "max_idle_polls"]
)
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_invalid_settings_are_rejected(field: str, value: object) -> None:
    with pytest.raises(ValueError, match=f"{field}는 1 이상의 정수"):
        make_operator(**{field: value})
