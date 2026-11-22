"""
Kafka 토픽 → S3 Parquet 적재 오퍼레이터

실행 시작 시점의 토픽 끝 offset까지 메시지를 읽어 Parquet으로 저장한 뒤 종료
S3 업로드 성공 후에만 offset을 commit하므로 전달은 at-least-once이고 중복은 Silver에서 제거

저장 경로:
  {s3_key_prefix}/{partition_by 값}_chunk_{묶음 토큰}.parquet
  partition_by가 없으면 {s3_key_prefix}/chunk_{묶음 토큰}.parquet

묶음 토큰은 묶음에 포함된 파티션별 시작 offset으로 만든다.
commit된 offset은 증가만 하므로 재시도해도 이미 commit한 파일을 덮어쓰지 않는다.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, NamedTuple, TypedDict

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from kafka import KafkaConsumer

import polars as pl
from airflow.models import BaseOperator
from hooks.kafka_hook import SteamKafkaHook
from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from kafka import TopicPartition

# Kafka 메시지 본문은 producer가 보낸 외부 응답 기반 JSON이라 필드 구조를 고정하지 않음
KafkaMessage = dict[str, Any]
EndOffsets = dict[TopicPartition, int]


class BufferedRecord(NamedTuple):
    """
    S3 저장 전 메모리에 모아 둔 메시지

    :param partition: Kafka 파티션 번호
    :param offset: 파티션 내 offset
    :param value: 역직렬화한 메시지 본문
    """

    partition: int
    offset: int
    value: KafkaMessage


class ConsumeSummary(TypedDict):
    """토픽 소비 결과 요약 (XCom 반환값)"""

    topic: str
    records: int
    files: int
    commits: int


class KafkaTopicToS3Operator(BaseOperator):
    """
    Kafka 토픽을 끝 offset까지 소비하여 S3에 Parquet으로 저장

    - 모든 파티션이 시작 시점의 끝 offset에 도달하면 종료하고, 이후 메시지는 다음 실행에서 처리
    - 끝 offset 직전 poll에 이후 메시지가 함께 오면 같은 묶음으로 저장
    - 메시지를 max_records_per_flush건씩 묶어 저장하고 묶음마다 offset commit
    - partition_by 필드 값별로 파일을 나눔 (예: 리뷰는 appid별 파일)

    :param kafka_topic: 소비할 Kafka 토픽명
    :param consumer_group_id: offset commit 기준 consumer group ID
    :param s3_key_prefix: S3 저장 경로 prefix (Jinja 템플릿 사용 가능)
    :param partition_by: 파일을 나눌 메시지 필드명. None이면 묶음당 파일 1개
    :param max_records_per_flush: 한 번에 저장하고 commit할 최대 메시지 수
    :param poll_timeout_ms: poll 1회 대기 시간
    :param max_idle_polls: 끝 offset 전에 허용하는 연속 빈 poll 횟수
    :param kafka_conn_id: Airflow에 등록한 Kafka 연결 ID
    :param aws_conn_id: Airflow에 등록한 S3 연결 ID
    :param s3_bucket: 대상 버킷
    """

    template_fields = ("s3_key_prefix",)

    def __init__(
        self,
        kafka_topic: str,
        consumer_group_id: str,
        s3_key_prefix: str,
        partition_by: str | None = None,
        max_records_per_flush: int = 10_000,
        poll_timeout_ms: int = 1_000,
        max_idle_polls: int = 30,
        kafka_conn_id: str = "kafka_default",
        aws_conn_id: str = "s3_storage",
        s3_bucket: str = BUCKET_RAW,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self._require_positive_int("max_records_per_flush", max_records_per_flush)
        self._require_positive_int("poll_timeout_ms", poll_timeout_ms)
        self._require_positive_int("max_idle_polls", max_idle_polls)

        self.kafka_topic = kafka_topic
        self.consumer_group_id = consumer_group_id
        self.s3_key_prefix = s3_key_prefix
        self.partition_by = partition_by
        self.max_records_per_flush = max_records_per_flush
        self.poll_timeout_ms = poll_timeout_ms
        self.max_idle_polls = max_idle_polls
        self.kafka_conn_id = kafka_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket

    @staticmethod
    def _require_positive_int(name: str, value: int) -> None:
        """설정값이 1 이상의 정수인지 검증"""
        # bool은 int의 하위형이라 True가 1로 통과하지 않도록 정확한 형을 비교
        if type(value) is not int or value < 1:
            raise ValueError(f"{name}는 1 이상의 정수여야 합니다: {name}={value!r}")

    # ── 실행 ──────────────────────────────────────

    def execute(self, context: Context) -> ConsumeSummary:  # noqa: ARG002
        consumer = SteamKafkaHook(kafka_conn_id=self.kafka_conn_id).create_consumer(
            group_id=self.consumer_group_id
        )
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)

        try:
            summary = self.drain(consumer, s3_hook)
        finally:
            # commit은 저장 성공 시점에만 수행하므로 종료 시 자동 commit 금지
            consumer.close(autocommit=False)

        self.log.info(
            "토픽 소비 완료: topic=%s  group=%s  메시지=%d건  파일=%d개  commit=%d회",
            summary["topic"],
            self.consumer_group_id,
            summary["records"],
            summary["files"],
            summary["commits"],
        )
        return summary

    def drain(self, consumer: KafkaConsumer, s3_hook: SteamS3Hook) -> ConsumeSummary:
        """
        시작 시점의 끝 offset까지 소비하며 묶음 단위로 저장·commit

        :param consumer: auto commit을 끈 consumer
        :param s3_hook: 저장 대상 S3 Hook
        :return: 소비 결과 요약
        :raises RuntimeError: 끝 offset 도달 전 빈 poll이 max_idle_polls회 연속 발생
        """
        summary: ConsumeSummary = {
            "topic": self.kafka_topic,
            "records": 0,
            "files": 0,
            "commits": 0,
        }

        partitions = consumer.partitions_for_topic(self.kafka_topic)
        if not partitions:
            self.log.warning(
                "토픽 파티션 없음, 소비 건너뜀: topic=%s", self.kafka_topic
            )
            return summary

        topic_partitions = [
            TopicPartition(self.kafka_topic, partition)
            for partition in sorted(partitions)
        ]
        consumer.assign(topic_partitions)
        end_offsets: EndOffsets = consumer.end_offsets(topic_partitions)

        buffer: list[BufferedRecord] = []
        idle_polls = 0

        while not self._reached_end(consumer, end_offsets):
            # poll 1회가 기본 500건까지 반환하므로 남은 묶음 용량만큼만 받아
            # 묶음 크기가 max_records_per_flush를 넘지 않게 한다
            remaining_capacity = self.max_records_per_flush - len(buffer)
            polled = consumer.poll(
                timeout_ms=self.poll_timeout_ms,
                max_records=remaining_capacity,
            )

            if not polled:
                idle_polls += 1
                if idle_polls >= self.max_idle_polls:
                    raise RuntimeError(
                        "끝 offset 도달 전 메시지 수신 중단: "
                        f"topic={self.kafka_topic}, 연속 빈 poll={idle_polls}회, "
                        f"위치={self._positions(consumer, end_offsets)}"
                    )
                continue

            idle_polls = 0
            for topic_partition, records in polled.items():
                buffer.extend(
                    BufferedRecord(
                        topic_partition.partition, record.offset, record.value
                    )
                    for record in records
                )

            if len(buffer) >= self.max_records_per_flush:
                self._flush_and_commit(consumer, s3_hook, buffer, summary)
                buffer = []

        if buffer:
            self._flush_and_commit(consumer, s3_hook, buffer, summary)

        return summary

    # ── 저장·commit ──────────────────────────────────────

    def _flush_and_commit(
        self,
        consumer: KafkaConsumer,
        s3_hook: SteamS3Hook,
        buffer: list[BufferedRecord],
        summary: ConsumeSummary,
    ) -> None:
        """
        묶음 전체를 S3에 저장한 뒤 현재 위치까지 offset commit

        - poll로 받은 메시지를 모두 묶음에 넣은 상태에서만 호출하므로
          consumer의 현재 위치가 곧 저장 완료 지점
        - 업로드가 하나라도 실패하면 commit하지 않고 예외 전파
        """
        files = self._upload_batch(s3_hook, buffer)
        consumer.commit()

        summary["records"] += len(buffer)
        summary["files"] += files
        summary["commits"] += 1

    def _upload_batch(self, s3_hook: SteamS3Hook, buffer: list[BufferedRecord]) -> int:
        """
        묶음을 partition_by 값별 Parquet 파일로 업로드

        :return: 업로드한 파일 수
        """
        token = self.batch_token(buffer)
        groups = self.group_records(buffer, self.partition_by)

        for group_key, rows in groups.items():
            key = self._object_key(group_key, token)
            frame = pl.DataFrame(rows, infer_schema_length=None)

            try:
                s3_hook.upload_parquet(df=frame, key=key, bucket=self.s3_bucket)
            except Exception:
                self.log.exception(
                    "S3 적재 실패, offset commit 보류: topic=%s  key=s3://%s/%s  메시지=%d건",
                    self.kafka_topic,
                    self.s3_bucket,
                    key,
                    len(rows),
                )
                raise

            self.log.info(
                "S3 적재: topic=%s  %d건 → s3://%s/%s",
                self.kafka_topic,
                len(rows),
                self.s3_bucket,
                key,
            )

        return len(groups)

    def _object_key(self, group_key: str | None, token: str) -> str:
        """그룹 값과 묶음 토큰으로 S3 객체 키 생성"""
        filename = (
            f"chunk_{token}.parquet"
            if group_key is None
            else f"{group_key}_chunk_{token}.parquet"
        )
        return f"{self.s3_key_prefix}/{filename}"

    # ── 데이터 변환 ──────────────────────────────────────

    @staticmethod
    def batch_token(buffer: list[BufferedRecord]) -> str:
        """
        묶음의 파티션별 시작 offset으로 파일명 토큰 생성

        - 같은 commit 지점에서 다시 읽은 묶음은 같은 시작 offset을 가져 같은 토큰
        - commit 이후 묶음은 시작 offset이 커져 이전 파일과 토큰이 겹치지 않음
        """
        start_offsets: dict[int, int] = {}
        for record in buffer:
            current = start_offsets.get(record.partition)
            if current is None or record.offset < current:
                start_offsets[record.partition] = record.offset

        identity = ",".join(
            f"p{partition}o{offset}"
            for partition, offset in sorted(start_offsets.items())
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def group_records(
        buffer: list[BufferedRecord],
        partition_by: str | None,
    ) -> dict[str | None, list[KafkaMessage]]:
        """
        메시지를 partition_by 필드 값별로 분류

        :param buffer: 저장할 메시지 묶음
        :param partition_by: 분류 기준 필드명. None이면 전체를 한 그룹으로 반환
        :return: 그룹 값별 메시지 본문 목록. partition_by가 None이면 키는 None
        :raises ValueError: 메시지에 partition_by 필드가 없거나 값이 비어 있음
        """
        if partition_by is None:
            return {None: [record.value for record in buffer]}

        groups: dict[str | None, list[KafkaMessage]] = {}
        for record in buffer:
            value = record.value.get(partition_by)
            if value is None or value == "":
                raise ValueError(
                    f"메시지에 분류 필드 없음: field={partition_by}, "
                    f"partition={record.partition}, offset={record.offset}"
                )
            groups.setdefault(str(value), []).append(record.value)
        return groups

    # ── 위치 확인 ──────────────────────────────────────

    @staticmethod
    def _reached_end(consumer: KafkaConsumer, end_offsets: EndOffsets) -> bool:
        """모든 파티션이 시작 시점의 끝 offset에 도달했는지 확인"""
        return all(
            consumer.position(topic_partition) >= end_offset
            for topic_partition, end_offset in end_offsets.items()
        )

    @staticmethod
    def _positions(consumer: KafkaConsumer, end_offsets: EndOffsets) -> dict[int, str]:
        """파티션별 현재 위치/끝 offset 표기 (오류 메시지용)"""
        return {
            topic_partition.partition: f"{consumer.position(topic_partition)}/{end_offset}"
            for topic_partition, end_offset in end_offsets.items()
        }
