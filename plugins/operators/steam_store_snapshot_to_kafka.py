"""
Steam 스토어 전체 스냅샷 → Kafka 발행 오퍼레이터

- 게임 목록(GetAppList)과 KR 가격·리뷰 요약(Query) 전체 순회를 Kafka 토픽으로 발행
- 동접자 수집(steam_players_to_kafka)도 같은 manifest 흐름을 사용
- 페이지마다 Kafka 전송 성공을 확인하고, 전체 순회가 끝나면 S3에 manifest 기록
- manifest가 있어야 Silver가 해당 스냅샷을 완료된 순회로 보고 구간을 갱신

manifest 경로: {snapshot_prefix}/_manifest.json
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.models import BaseOperator
from hooks.kafka_hook import SteamKafkaHook
from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from hooks.steam_api import SteamApiHook
from utils.player_watchlist import PlayerCountMessage
from utils.store_catalog import (
    MANIFEST_FILENAME,
    AppListMessage,
    SnapshotManifest,
    StorePriceMessage,
    normalize_store_item,
    to_app_list_message,
)

SnapshotMessage = AppListMessage | StorePriceMessage | PlayerCountMessage


class SnapshotPage(NamedTuple):
    """
    페이지 단위 변환 결과

    :param messages: 발행할 메시지
    :param skipped: 게임이 아니라 건너뛴 항목 수
    :param api_total: API가 알려 준 전체 건수 (없으면 None)
    """

    messages: list[SnapshotMessage]
    skipped: int
    api_total: int | None


class StoreSnapshotToKafkaOperator(BaseOperator):
    """
    전체 스냅샷 발행 공통 흐름

    - 하위 클래스는 source 이름과 iter_snapshot_pages만 정의
    - 같은 appid가 순회 중 다시 나오면 첫 항목만 발행하고 skipped로 집계

    :param kafka_topic: 발행할 Kafka 토픽명
    :param snapshot_id: 스냅샷 식별자 (Jinja 템플릿, 예: "{{ ts_nodash }}")
    :param snapshot_prefix: manifest·consumer 저장 경로 prefix (Jinja 템플릿)
    :param kafka_conn_id: Airflow에 등록한 Kafka 연결 ID
    :param aws_conn_id: Airflow에 등록한 S3 연결 ID
    :param s3_bucket: manifest 저장 버킷
    """

    source = ""
    template_fields = ("snapshot_id", "snapshot_prefix")

    def __init__(
        self,
        kafka_topic: str,
        snapshot_id: str,
        snapshot_prefix: str,
        kafka_conn_id: str = "kafka_default",
        aws_conn_id: str = "s3_storage",
        s3_bucket: str = BUCKET_RAW,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.kafka_topic = kafka_topic
        self.snapshot_id = snapshot_id
        self.snapshot_prefix = snapshot_prefix
        self.kafka_conn_id = kafka_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket

    def iter_snapshot_pages(
        self,
        api_hook: SteamApiHook,
        collected_at: int,
    ) -> Iterator[SnapshotPage]:
        """API 페이지를 메시지 목록으로 변환해 순회 (하위 클래스 구현)"""
        raise NotImplementedError

    # ── 실행 ──────────────────────────────────────

    def execute(self, context: Context) -> SnapshotManifest:  # noqa: ARG002
        kafka_hook = SteamKafkaHook(kafka_conn_id=self.kafka_conn_id)
        try:
            manifest = self.publish(SteamApiHook(), kafka_hook)
        finally:
            kafka_hook.close()

        manifest_key = f"{self.snapshot_prefix}/{MANIFEST_FILENAME}"
        SteamS3Hook(aws_conn_id=self.aws_conn_id).upload_json(
            data=dict(manifest),
            key=manifest_key,
            bucket=self.s3_bucket,
        )
        self.log.info(
            "스냅샷 발행 완료: source=%s  snapshot=%s  메시지=%d건  건너뜀=%d건  API 전체=%s  manifest=s3://%s/%s",
            self.source,
            self.snapshot_id,
            manifest["records"],
            manifest["skipped"],
            manifest["api_total"],
            self.s3_bucket,
            manifest_key,
        )
        return manifest

    def publish(
        self, api_hook: SteamApiHook, kafka_hook: SteamKafkaHook
    ) -> SnapshotManifest:
        """
        전체 순회를 발행하고 manifest 내용 반환

        - 페이지마다 flush로 전송 성공을 확인해 실패 시 manifest를 남기지 않음

        :return: Kafka 전송이 모두 확인된 스냅샷의 manifest
        """
        collected_at = int(time.time())
        seen: set[int] = set()
        skipped = 0
        api_total: int | None = None

        for page in self.iter_snapshot_pages(api_hook, collected_at):
            skipped += page.skipped
            api_total = page.api_total

            for message in page.messages:
                appid = message["appid"]
                if appid in seen:
                    skipped += 1
                    continue
                seen.add(appid)
                kafka_hook.send(
                    topic=self.kafka_topic, value=dict(message), key=str(appid)
                )

            kafka_hook.flush()

        return {
            "source": self.source,
            "snapshot_id": self.snapshot_id,
            "collected_at": collected_at,
            "records": len(seen),
            "api_total": api_total,
            "skipped": skipped,
        }


class SteamAppListToKafkaOperator(StoreSnapshotToKafkaOperator):
    """GetAppList 전체 게임 목록 발행 (DLC·소프트웨어·영상·하드웨어 제외)"""

    source = "app_list"

    def iter_snapshot_pages(
        self,
        api_hook: SteamApiHook,
        collected_at: int,
    ) -> Iterator[SnapshotPage]:
        for apps in api_hook.iter_app_list_pages(include_games=True):
            messages: list[SnapshotMessage] = [
                to_app_list_message(app, self.snapshot_id, collected_at) for app in apps
            ]
            yield SnapshotPage(messages, skipped=0, api_total=None)


class SteamStorePricesToKafkaOperator(StoreSnapshotToKafkaOperator):
    """
    Query 전체 순회로 KR 가격·리뷰 요약 발행

    :param country_code: 가격 기준 국가 코드
    :param language: 응답 언어
    """

    source = "store_prices"

    def __init__(
        self,
        country_code: str = "KR",
        language: str = "english",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.country_code = country_code
        self.language = language

    def iter_snapshot_pages(
        self,
        api_hook: SteamApiHook,
        collected_at: int,
    ) -> Iterator[SnapshotPage]:
        pages = api_hook.iter_store_query_pages(
            query_name="store_prices_full_scan",
            filters={"type_filters": {"include_games": True}},
            context={"language": self.language, "country_code": self.country_code},
            data_request={
                "include_all_purchase_options": True,
                "include_reviews": True,
            },
        )
        for items, total in pages:
            messages: list[SnapshotMessage] = []
            skipped = 0
            for item in items:
                message = normalize_store_item(item, self.snapshot_id, collected_at)
                if message is None:
                    skipped += 1
                    continue
                messages.append(message)
            yield SnapshotPage(messages, skipped=skipped, api_total=total)
