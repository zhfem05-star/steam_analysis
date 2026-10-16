"""
Steam 리뷰 수집 → Kafka 발행 오퍼레이터

tracked_games의 언어별 체크포인트를 기준으로 과거·증분 리뷰 수집
Kafka 전송 성공 확인 후 review_cursors와 reviews_collected_at 갱신

수집 구조:
  - Producer(메인 스레드): 언어별 API 페이지 요청 → 큐 적재 → rate limit 대기
  - Consumer(별도 스레드): 큐 소비 → Kafka 발행 확인 → 체크포인트 저장
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from callbacks.slack_callback import slack_collect_summary
from hooks.kafka_hook import SteamKafkaHook
from hooks.steam_api import SteamApiHook
from utils.review_collection import ReviewCollector
from utils.review_collection import ReviewPage as _PageItem
from utils.review_types import (
    CollectionMode,
    LanguageCheckpoints,
    SavedCheckpoint,
    WindowCheckpoint,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

# 커서 체크포인트 주기 (몇 건 발행마다 DB에 커서를 기록할지)
_CURSOR_UPDATE_INTERVAL = 1000
# queue에 대기 가능한 최대 페이지 수 (메모리 상한)
_QUEUE_MAXSIZE = 5
# 관측된 10만 건 cursor 한계에 도달하기 전에 날짜 경계를 이동한다.
_WINDOW_REVIEW_LIMIT = 50000


class SteamReviewsToKafkaOperator(BaseOperator):
    """
    tracked_games의 체크포인트를 기준으로 과거·증분 리뷰 수집 및 Kafka 발행

    수집 완료(또는 일정 건수 발행마다) tracked_games.review_cursors,
    reviews_collected_at을 갱신하여 다음 실행의 재개 위치 기록

    :param kafka_topic:      발행할 Kafka 토픽명
    :param app_ids:          수집할 게임 ID 목록 (fixed 모드에서만 DB 조회 생략)
    :param app_id_limit:     조회할 app_id 수 제한. None이면 전체 조회
    :param languages:        수집할 언어 목록. 기본값 ["korean", "english"]
    :param filter_type:      Steam 리뷰 정렬 방식
                             "recent"  : 최신순 (기본값)
                             "all"     : Steam 추천 알고리즘 순
                             "updated" : 최근 수정된 리뷰 순
    :param date_range_type: 날짜 범위 적용 방식 ("include": 지정 범위 안의 리뷰 조회)
    :param start_date:       수집 시작 시각 (초 단위 Unix timestamp)
    :param end_date:         수집 종료 시각 (초 단위 Unix timestamp)
    :param window_days:      기본 날짜 구간 길이 (일 단위, 기본값: 30)
    :param collection_mode:  수집 모드 (fixed: 고정 기간, continuous: 과거 수집 후 증분 수집)
    :param incremental_overlap_seconds: 증분 수집 중첩 시간 (초 단위, 기본값: 86400)
    :param postgres_conn_id: Airflow에 등록한 Postgres 연결 ID
    :param kafka_conn_id:    Airflow에 등록한 Kafka 연결 ID
    """

    # ── 수집 설정 및 검증 ──────────────────────────────────────

    def __init__(
        self,
        kafka_topic: str = "steam-reviews",
        app_ids: list[int] | None = None,
        app_id_limit: int | None = None,
        languages: list[str] | None = None,
        filter_type: str = "recent",
        postgres_conn_id: str = "analytics_db",
        kafka_conn_id: str = "kafka_default",
        date_range_type: str | None = None,
        start_date: int | None = None,
        end_date: int | None = None,
        window_days: int = 30,
        collection_mode: CollectionMode = "fixed",
        incremental_overlap_seconds: int = 86400,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.kafka_topic = kafka_topic
        self.app_ids = app_ids
        self.app_id_limit = app_id_limit
        self.languages = languages or ["korean", "english"]
        self.filter_type = filter_type
        self.date_range_type = date_range_type
        self.start_date = start_date
        self.end_date = end_date

        # 날짜 구간 크기와 수집 모드 검증
        if (
            isinstance(window_days, bool)
            or not isinstance(window_days, int)
            or window_days < 1
        ):
            raise ValueError("window_days는 1 이상의 정수여야 합니다")
        self.window_days = window_days

        if collection_mode not in ("fixed", "continuous"):
            raise ValueError("collection_mode는 fixed 또는 continuous여야 합니다")

        if (
            type(incremental_overlap_seconds) is not int
            or incremental_overlap_seconds < 1
        ):
            raise ValueError("incremental_overlap_seconds는 1 이상의 정수여야 합니다")
        self.collection_mode = collection_mode
        self.incremental_overlap_seconds = incremental_overlap_seconds

        # 날짜 필터 조합과 수집 범위 검증
        dated = any(
            value is not None for value in (date_range_type, start_date, end_date)
        )
        if dated or collection_mode == "continuous":
            if date_range_type != "include" or filter_type != "recent":
                raise ValueError(
                    "날짜 구간 수집에는 date_range_type=include, filter_type=recent가 필요합니다"
                )

            bounds = (
                (start_date, end_date) if collection_mode == "fixed" else (start_date,)
            )
            if any(type(value) is not int for value in bounds):
                raise ValueError("수집 날짜 경계는 정수 Unix timestamp여야 합니다")

            # 위 검사에서 시작 시각이 정수임을 확인했으므로 타입 검사기에도 명시한다.
            start_timestamp = cast(int, start_date)
            if start_timestamp <= 0 or (
                end_date is not None and start_timestamp > end_date
            ):
                raise ValueError(
                    "수집 날짜는 0 < start_date <= end_date여야 합니다 (Steam은 시작 시각 0을 무시)"
                )

        if collection_mode == "continuous" and end_date is not None:
            raise ValueError(
                "continuous 모드는 data_interval_end를 사용하므로 end_date를 지정할 수 없습니다"
            )

        self.postgres_conn_id = postgres_conn_id
        self.kafka_conn_id = kafka_conn_id

    @staticmethod
    def _determine_workers(target_count: int) -> int:
        """
        Steam rate limit을 고려한 작업 스레드 수 산정 (최대 8개)
        """
        cpu_count = os.cpu_count() or 1
        ideal = cpu_count + 4
        return min(ideal, 8, target_count)

    # ── 게임별 병렬 실행 및 결과 보고 ──────────────────────────

    def execute(self, context: Context) -> None:
        if self.collection_mode == "continuous":
            cutoff = context.get("data_interval_end") or datetime.now(timezone.utc)
            if cutoff.tzinfo is None:
                raise ValueError("data_interval_end에는 시간대 정보가 필요합니다")
            self._run_end = int(cutoff.timestamp())
            if self._run_end < cast(int, self.start_date):
                raise ValueError("실행 종료 시각이 수집 시작 시각보다 이릅니다")

        targets = self._fetch_targets()

        if not targets:
            raise ValueError(
                "tracked_games 테이블에서 수집 대상 app_id를 가져오지 못했습니다."
            )

        max_workers = self._determine_workers(len(targets))
        self.log.info(
            "수집 대상: %d개 게임 (limit=%s)  workers=%d",
            len(targets),
            self.app_id_limit,
            max_workers,
        )

        succeeded_ids: list[int] = []
        failed_ids: list[int] = []

        # 앱마다 hook을 분리해 전송 실패나 미확인 Future가 다른 앱에 영향을 주지 않게 한다.
        def collect_one(appid: int, saved_cursors: LanguageCheckpoints) -> None:
            api_hook = SteamApiHook()
            kafka_hook = SteamKafkaHook(kafka_conn_id=self.kafka_conn_id)
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            try:
                self._collect_and_publish(
                    appid, saved_cursors, api_hook, kafka_hook, pg_hook
                )
            finally:
                kafka_hook.close()

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
                except Exception:
                    self.log.exception("게임 리뷰 수집 실패: appid=%s", appid)
                    failed_ids.append(appid)

        ti = context["ti"]
        slack_collect_summary(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            log_url=ti.log_url,
            succeeded=succeeded_ids,
            failed=failed_ids,
        )

        if failed_ids:
            raise RuntimeError(
                f"게임 리뷰 수집 실패: 실패 수={len(failed_ids)}, appids={failed_ids}"
            )

    # ── 수집기 구성 및 페이지 순회 ──────────────────────────────

    def _collector(self) -> ReviewCollector:
        return ReviewCollector(
            start_date=self.start_date,
            end_date=self.end_date,
            filter_type=self.filter_type,
            date_range_type=self.date_range_type,
            window_days=self.window_days,
            review_limit=_WINDOW_REVIEW_LIMIT,
            incremental_overlap_seconds=self.incremental_overlap_seconds,
        )

    def _review_checkpoint(self, saved: SavedCheckpoint) -> WindowCheckpoint:
        return self._collector().checkpoint(saved)

    def _iter_review_items(
        self,
        appid: int,
        language: str,
        saved: SavedCheckpoint,
        api_hook: SteamApiHook,
    ) -> Iterator[_PageItem]:
        collector = self._collector()
        if self.collection_mode == "continuous":
            yield from collector.continuous_pages(
                appid, language, saved, api_hook, self._run_end
            )
        else:
            yield from collector.pages(appid, language, saved, api_hook)

    # ── API 수집 및 Kafka 발행 ─────────────────────────────────

    def _collect_and_publish(
        self,
        appid: int,
        saved_cursors: LanguageCheckpoints,
        api_hook: SteamApiHook,
        kafka_hook: SteamKafkaHook,
        pg_hook: PostgresHook,
    ) -> None:
        """
        Producer-Consumer 패턴으로 리뷰를 수집하고 Kafka에 발행

        Producer(메인 스레드): 언어별 API 페이지 요청 → queue 적재 → rate limit 대기
        Consumer(별도 스레드): queue에서 꺼내 Kafka로 발행 → N건마다 cursor 체크포인트
        """
        page_queue: queue.Queue[_PageItem | None] = queue.Queue(maxsize=_QUEUE_MAXSIZE)
        errors: list[Exception] = []
        stopped = threading.Event()

        def enqueue(item: _PageItem | None) -> bool:
            while not stopped.is_set():
                try:
                    page_queue.put(item, timeout=0.1)
                    return True
                except queue.Full:
                    continue
            return False

        # ── Producer ──
        def producer() -> None:
            try:
                for language in self.languages:
                    for item in self._iter_review_items(
                        appid,
                        language,
                        saved_cursors.get(language),
                        api_hook,
                    ):
                        if not enqueue(item):
                            return
            except Exception as error:
                self.log.exception("Steam API 페이지 수집 실패: appid=%s", appid)
                errors.append(error)
            finally:
                enqueue(None)  # consumer 종료 신호

        # ── Consumer ──
        def consumer() -> None:
            current_cursors: LanguageCheckpoints = dict(saved_cursors)
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
                            value={
                                **review,
                                "appid": appid,
                                "language": review.get("language") or item.language,
                                "_collected_at": time.time_ns() // 1000,
                            },
                            key=str(appid),
                        )

                    total_published += len(item.reviews)
                    published_since_checkpoint += len(item.reviews)
                    current_cursors[item.language] = item.checkpoint

                    checkpoint_due = (
                        not item.reviews
                        or published_since_checkpoint >= _CURSOR_UPDATE_INTERVAL
                    )
                    if checkpoint_due:
                        # Kafka 전송 성공 확인 후에만 DB의 진행 위치 갱신
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
                        appid,
                        self.kafka_topic,
                        total_published,
                    )

            except Exception as error:
                self.log.exception(
                    "Kafka 발행 또는 체크포인트 저장 실패: appid=%s, topic=%s",
                    appid,
                    self.kafka_topic,
                )
                errors.append(error)
                stopped.set()

        # 소비 스레드 시작 → 메인 스레드 수집 → 소비 종료 확인
        consumer_thread = threading.Thread(target=consumer, daemon=True)
        consumer_thread.start()
        producer()  # 메인 스레드가 producer 역할
        consumer_thread.join()

        if errors:
            raise errors[0]

    # ── 수집 대상 조회 및 체크포인트 저장 ───────────────────────

    def _fetch_targets(self) -> list[tuple[int, LanguageCheckpoints]]:
        """
        수집 대상 게임과 언어별 체크포인트 조회

        fixed 모드에서 app_ids 지정 시 DB 조회 없이 해당 목록 사용
        continuous 모드에서는 DB에 등록된 활성 수집 대상과 체크포인트 사용

        :return: (appid, 언어별 체크포인트) 목록
        """
        if self.app_ids is not None and self.collection_mode == "fixed":
            return [(appid, {}) for appid in self.app_ids]

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        base_sql = """
            SELECT appid, review_cursors
            FROM tracked_games
            WHERE is_active = TRUE AND collect_reviews = TRUE
            ORDER BY last_discounted DESC
        """
        if self.app_ids is not None:
            sql = base_sql.replace(
                "ORDER BY last_discounted DESC", "AND appid = ANY(%s)"
            )
            rows = pg_hook.get_records(sql, parameters=(self.app_ids,))
            if {row[0] for row in rows} != set(self.app_ids):
                raise ValueError(
                    "continuous 수집 대상은 tracked_games에 활성화된 리뷰 수집 대상으로 등록되어야 합니다"
                )
        elif self.app_id_limit is not None:
            sql = base_sql + " LIMIT %s"
            rows = pg_hook.get_records(sql, parameters=(self.app_id_limit,))
        else:
            rows = pg_hook.get_records(base_sql)

        result: list[tuple[int, LanguageCheckpoints]] = []
        for appid, cursors_raw in rows:
            if isinstance(cursors_raw, str):
                cursors = json.loads(cursors_raw)
            else:
                cursors = cursors_raw or {}
            result.append((appid, cast(LanguageCheckpoints, cursors)))

        return result

    def _update_cursor(
        self,
        pg_hook: PostgresHook,
        appid: int,
        next_cursors: LanguageCheckpoints,
    ) -> None:
        """
        cursor와 수집 시각을 tracked_games에 기록
        """
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
