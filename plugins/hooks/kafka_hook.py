from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kafka.producer.future import FutureRecordMetadata

from airflow.hooks.base import BaseHook
from kafka import KafkaProducer


class SteamKafkaHook:
    """
    Kafka 프로듀서 연결을 위한 프로젝트 전용 Hook
    """

    def __init__(self, kafka_conn_id: str = "kafka_default") -> None:
        self.kafka_conn_id = kafka_conn_id
        self._producer: KafkaProducer | None = None
        self._pending: list[FutureRecordMetadata] = []
        self.delivery_timeout: float = 60.0

    def _get_bootstrap_servers(self) -> str:
        conn = BaseHook.get_connection(self.kafka_conn_id)
        return f"{conn.host}:{conn.port}"

    def get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self._get_bootstrap_servers(),
                acks="all",
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                    "utf-8"
                ),
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            )
        return self._producer

    # ── 메시지 발행 및 전송 확인 ───────────────────────────────

    def send(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        """
        메시지 비동기 발행 및 전송 결과 추적
        """
        future = self.get_producer().send(topic, value=value, key=key)
        self._pending.append(future)

    def flush(self) -> None:
        """
        대기 중인 메시지의 Kafka 전송 성공 확인

        전송 실패 시 Future 목록을 유지하여 체크포인트 전진 방지
        """
        if self._producer is not None:
            deadline = time.monotonic() + self.delivery_timeout
            self._producer.flush(timeout=self.delivery_timeout)
            # flush는 실패한 전송까지 포함해 처리가 끝나기만 기다린다.
            # 모든 Future의 전송 성공을 확인해야 체크포인트를 전진시킬 수 있다.
            for future in self._pending:
                future.get(timeout=max(0, deadline - time.monotonic()))
            self._pending.clear()

    def close(self) -> None:
        """
        제한 시간 내 Kafka 프로듀서 연결 종료
        """
        if self._producer is not None:
            self._producer.close(timeout=self.delivery_timeout)
            self._producer = None
