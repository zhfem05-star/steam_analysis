import json

from airflow.hooks.base import BaseHook
from kafka import KafkaProducer


class SteamKafkaHook:
    """Kafka 프로듀서 연결을 위한 프로젝트 전용 Hook"""

    def __init__(self, kafka_conn_id: str = "kafka_default"):
        self.kafka_conn_id = kafka_conn_id
        self._producer: KafkaProducer | None = None

    def _get_bootstrap_servers(self) -> str:
        conn = BaseHook.get_connection(self.kafka_conn_id)
        return f"{conn.host}:{conn.port}"

    def get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self._get_bootstrap_servers(),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            )
        return self._producer

    def send(self, topic: str, value: dict, key: str | None = None) -> None:
        self.get_producer().send(topic, value=value, key=key)

    def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
            self._producer = None
