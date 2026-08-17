"""
Steam 리뷰 수집 → Kafka 발행 오퍼레이터

tracked_games 컨트롤 테이블에서 app_id와 언어별 cursor를 조회하여
마지막 수집 이후 새로 추가된 한국어·영어 리뷰를 증분 수집하고 Kafka 토픽에 발행한다.
발행된 토픽은 Spark Structured Streaming이 구독하여 fact_review_daily 등으로 집계한다.

[수집 구조]
Producer(메인 흐름) — API 페이지 요청 → queue에 적재 → rate limit 대기
Consumer(별도 스레드) — queue에서 꺼내 Kafka로 발행, 일정 건수마다 cursor 체크포인트 기록

rate limit 대기(1.5초) 동안 consumer가 Kafka 발행을 병렬로 처리하여
대기 시간을 유효하게 활용한다.
"""

from __future__ import annotations

import json
import os
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import NamedTuple

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from callbacks.slack_callback import slack_collect_summary
from hooks.kafka_hook import SteamKafkaHook
from hooks.steam_api import SteamApiHook

# 커서 체크포인트 주기 (몇 건 발행마다 DB에 커서를 기록할지)
_CURSOR_UPDATE_INTERVAL = 1000
# queue에 대기 가능한 최대 페이지 수 (메모리 상한)
_QUEUE_MAXSIZE = 5


class _PageItem(NamedTuple):
    language: str
    reviews: list[dict]
    next_cursor: str


class SteamReviewsToKafkaOperator(BaseOperator):
    """
    tracked_games 테이블의 cursor를 기준으로 새 리뷰만 증분 수집 후 Kafka 토픽에 발행.

    수집 완료(또는 일정 건수 발행마다) tracked_games.review_cursors,
    reviews_collected_at을 갱신하여 다음 실행에서 이어서 수집할 수 있다.

    :param kafka_topic:      발행할 Kafka 토픽명
    :param app_ids:          지정 시 DB 조회 없이 이 목록만 수집 (테스트·성능 비교용)
    :param app_id_limit:     조회할 app_id 수 제한. None이면 전체 조회
    :param languages:        수집할 언어 목록. 기본값 ["korean", "english"]
    :param filter_type:      Steam 리뷰 정렬 방식.
                             "recent"  : 최신순 (기본값)
                             "all"     : Steam 추천 알고리즘 순
                             "updated" : 최근 수정된 리뷰 순
    :param postgres_conn_id: Airflow Postgres Connection ID
    :param kafka_conn_id:    Airflow Kafka Connection ID
    """

    def __init__(
        self,
        kafka_topic: str = "steam-reviews",
        app_ids: list[int] | None = None,
        app_id_limit: int | None = None,
        languages: list[str] | None = None,
        filter_type: str = "recent",
        postgres_conn_id: str = "analytics_db",
        kafka_conn_id: str = "kafka_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.kafka_topic = kafka_topic
        self.app_ids = app_ids
        self.app_id_limit = app_id_limit
        self.languages = languages or ["korean", "english"]
        self.filter_type = filter_type
        self.postgres_conn_id = postgres_conn_id
        self.kafka_conn_id = kafka_conn_id

    @staticmethod
    def _determine_workers(target_count: int) -> int:
        """I/O bound 작업이므로 CPU 수보다 많은 스레드 사용. Steam rate limit 고려 최대 8개."""
        cpu_count = os.cpu_count() or 1
        ideal = cpu_count + 4
        return min(ideal, 8, target_count)

    def execute(self, context):
        targets = self._fetch_targets()

        if not targets:
            raise ValueError("tracked_games 테이블에서 수집 대상 app_id를 가져오지 못했습니다.")

        max_workers = self._determine_workers(len(targets))
        self.log.info(
            "수집 대상: %d개 게임 (limit=%s)  workers=%d",
            len(targets), self.app_id_limit, max_workers,
        )

        succeeded_ids: list[int] = []
        failed_ids: list[int] = []

        # 스레드별 독립 hook 인스턴스 (thread-local storage)
        thread_local = threading.local()

        def get_hooks():
            if not hasattr(thread_local, "api_hook"):
                thread_local.api_hook = SteamApiHook()
                thread_local.kafka_hook = SteamKafkaHook(kafka_conn_id=self.kafka_conn_id)
                thread_local.pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            return thread_local.api_hook, thread_local.kafka_hook, thread_local.pg_hook

        def collect_one(appid: int, saved_cursors: dict):
            self.log.info("리뷰 수집 시작: appid=%s  cursor=%s", appid, saved_cursors)
            api_hook, kafka_hook, pg_hook = get_hooks()
            self._collect_and_publish(appid, saved_cursors, api_hook, kafka_hook, pg_hook)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_appid = {
                executor.submit(collect_one, appid, saved_cursors): appid
                for appid, saved_cursors in targets
            }
            for future in as_completed(future_to_appid):
                appid = future_to_appid[future]
                try:
                    future.result()
                    succeeded_ids.append(appid)
                except Exception as e:
                    self.log.error("appid=%s 수집 실패: %s", appid, e)
                    failed_ids.append(appid)

        ti = context["ti"]
        slack_collect_summary(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            log_url=ti.log_url,
            succeeded=succeeded_ids,
            failed=failed_ids,
        )

        if len(failed_ids) == len(targets):
            raise RuntimeError(
                f"리뷰 수집 전체 실패: {len(failed_ids)}개 (appid={failed_ids})"
            )

    # ── 내부 메서드 ──────────────────────────────────────

    def _collect_and_publish(
        self,
        appid: int,
        saved_cursors: dict,
        api_hook: SteamApiHook,
        kafka_hook: SteamKafkaHook,
        pg_hook: PostgresHook,
    ) -> None:
        """
        Producer-Consumer 패턴으로 리뷰를 수집하고 Kafka에 발행.

        Producer(메인 스레드): 언어별 API 페이지 요청 → queue 적재 → rate limit 대기
        Consumer(별도 스레드): queue에서 꺼내 Kafka로 발행 → N건마다 cursor 체크포인트
        """
        page_queue: queue.Queue = queue.Queue(maxsize=_QUEUE_MAXSIZE)
        errors: list[Exception] = []

        # ── Producer ──
        def producer():
            try:
                for language in self.languages:
                    start_cursor = saved_cursors.get(language, "*")
                    for page_reviews, next_cursor in api_hook.iter_review_pages(
                        appid=appid,
                        language=language,
                        start_cursor=start_cursor,
                        filter_type=self.filter_type,
                    ):
                        page_queue.put(_PageItem(language, page_reviews, next_cursor))
            except Exception as e:
                errors.append(e)
            finally:
                page_queue.put(None)  # consumer 종료 신호

        # ── Consumer ──
        def consumer():
            current_cursors: dict = dict(saved_cursors)
            published_since_checkpoint = 0
            total_published = 0

            try:
                while True:
                    item = page_queue.get()
                    if item is None:
                        break

                    for review in item.reviews:
                        kafka_hook.send(
                            topic=self.kafka_topic,
                            value={"appid": appid, "language": item.language, **review},
                            key=str(appid),
                        )

                    total_published += len(item.reviews)
                    published_since_checkpoint += len(item.reviews)
                    current_cursors[item.language] = item.next_cursor

                    if published_since_checkpoint >= _CURSOR_UPDATE_INTERVAL:
                        kafka_hook.flush()
                        self._update_cursor(pg_hook, appid, current_cursors)
                        published_since_checkpoint = 0

                # 남은 발행분 flush 및 최종 cursor 기록
                kafka_hook.flush()
                self._update_cursor(pg_hook, appid, current_cursors)

                if total_published == 0:
                    self.log.warning("새 리뷰 없음: appid=%s", appid)
                else:
                    self.log.info(
                        "Kafka 발행 완료: appid=%s  topic=%s  총=%d건",
                        appid, self.kafka_topic, total_published,
                    )

            except Exception as e:
                errors.append(e)

        consumer_thread = threading.Thread(target=consumer, daemon=True)
        consumer_thread.start()
        producer()               # 메인 스레드가 producer 역할
        consumer_thread.join()

        if errors:
            raise errors[0]

    def _fetch_targets(self) -> list[tuple[int, dict]]:
        """
        수집 대상 (appid, review_cursors) 목록 반환.

        app_ids가 지정된 경우 DB 조회 없이 해당 목록만 사용 (테스트·성능 비교용).
        미지정 시 tracked_games 테이블에서 조회.
        """
        if self.app_ids is not None:
            return [(appid, {}) for appid in self.app_ids]

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        base_sql = """
            SELECT appid, review_cursors
            FROM tracked_games
            WHERE is_active = TRUE AND collect_reviews = TRUE
            ORDER BY last_discounted DESC
        """
        if self.app_id_limit is not None:
            sql = base_sql + " LIMIT %s"
            rows = pg_hook.get_records(sql, parameters=(self.app_id_limit,))
        else:
            rows = pg_hook.get_records(base_sql)

        result = []
        for appid, cursors_raw in rows:
            if isinstance(cursors_raw, str):
                cursors = json.loads(cursors_raw)
            else:
                cursors = cursors_raw or {}
            result.append((appid, cursors))

        return result

    def _update_cursor(
        self,
        pg_hook: PostgresHook,
        appid: int,
        next_cursors: dict[str, str],
    ) -> None:
        """cursor와 수집 시각을 tracked_games에 기록."""
        collected_at = datetime.now(timezone.utc)
        pg_hook.run(
            """
            UPDATE tracked_games
               SET review_cursors       = %s,
                   reviews_collected_at = %s
             WHERE appid = %s
            """,
            parameters=(json.dumps(next_cursors), collected_at, appid),
        )
        self.log.info("cursor 갱신: appid=%s  cursors=%s", appid, next_cursors)
