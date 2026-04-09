"""
Steam 리뷰 수집 → S3 저장 오퍼레이터

tracked_games 컨트롤 테이블에서 app_id와 언어별 cursor를 조회하여
마지막 수집 이후 새로 추가된 한국어·영어 리뷰를 증분 수집하고 MinIO(S3)에 저장한다.

[수집 구조]
Producer(메인 흐름) — API 페이지 요청 → queue에 적재 → rate limit 대기
Consumer(별도 스레드) — queue에서 꺼내 chunk 버퍼에 누적 → CHUNK_SIZE 도달 시 S3 저장

rate limit 대기(1.5초) 동안 consumer가 S3 write를 병렬로 처리하여
대기 시간을 유효하게 활용한다.
chunk 단위로 저장하므로 메모리 사용량이 CHUNK_SIZE로 고정된다.

저장 경로: {s3_key_prefix}/{appid}_chunk_{N:03d}.json
"""

from __future__ import annotations

import json
import os
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import NamedTuple

import polars as pl

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from callbacks.slack_callback import slack_collect_summary
from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from hooks.steam_api import SteamApiHook

# S3 파일 하나에 담을 최대 리뷰 수
_CHUNK_SIZE = 1000
# queue에 대기 가능한 최대 페이지 수 (메모리 상한)
_QUEUE_MAXSIZE = 5


class _PageItem(NamedTuple):
    language: str
    reviews: list[dict]
    next_cursor: str


class SteamReviewsToS3Operator(BaseOperator):
    """
    tracked_games 테이블의 cursor를 기준으로 새 리뷰만 증분 수집 후 S3 청크 저장.

    수집 완료 시 tracked_games.review_cursors, reviews_collected_at을 갱신하여
    다음 실행에서 이어서 수집할 수 있다.

    :param s3_key_prefix:    S3 저장 경로 prefix (Jinja 템플릿 사용 가능)
                             예: "reviews/{{ execution_date.strftime('%Y%m%d_%H%M') }}"
    :param app_id_limit:     조회할 app_id 수 제한. None이면 전체 조회 (테스트 시 소수 지정)
    :param languages:        수집할 언어 목록. 기본값 ["korean", "english"]
    :param filter_type:      Steam 리뷰 정렬 방식.
                             "recent"  : 최신순 (기본값)
                             "all"     : Steam 추천 알고리즘 순
                             "updated" : 최근 수정된 리뷰 순
    :param postgres_conn_id: Airflow Postgres Connection ID
    :param s3_bucket:        대상 버킷 (기본값: steam-raw)
    :param aws_conn_id:      Airflow S3 Connection ID
    """

    template_fields = ("s3_key_prefix",)

    def __init__(
        self,
        s3_key_prefix: str,
        app_ids: list[int] | None = None,
        app_id_limit: int | None = None,
        languages: list[str] | None = None,
        filter_type: str = "recent",
        postgres_conn_id: str = "analytics_db",
        s3_bucket: str = BUCKET_RAW,
        aws_conn_id: str = "minio_s3",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_key_prefix = s3_key_prefix
        self.app_ids = app_ids          # 지정 시 DB 조회 없이 이 목록만 수집
        self.app_id_limit = app_id_limit
        self.languages = languages or ["korean", "english"]
        self.filter_type = filter_type
        self.postgres_conn_id = postgres_conn_id
        self.s3_bucket = s3_bucket
        self.aws_conn_id = aws_conn_id

    @staticmethod
    def _determine_workers(target_count: int) -> int:
        """
        환경에 맞는 worker 수를 동적으로 결정.

        I/O bound 작업이므로 CPU 수보다 더 많은 스레드를 사용해도 효율적.
        단, Steam rate limit을 고려해 최대 8개로 제한.
        실제 대상 게임 수보다 많은 worker는 의미 없으므로 target_count로 상한.
        """
        cpu_count = os.cpu_count() or 1
        # I/O bound: CPU 수 + 4 (Python ThreadPoolExecutor 공식 기본값)
        ideal = cpu_count + 4
        # Steam rate limit 고려 상한 및 실제 대상 수 초과 방지
        return min(ideal, 8, target_count)

    def execute(self, context):
        targets = self._fetch_targets()

        if not targets:
            raise ValueError("tracked_games 테이블에서 수집 대상 app_id를 가져오지 못했습니다.")

        max_workers = self._determine_workers(len(targets))
        self.log.info(
            "수집 대상: %d개 게임 (limit=%s)  workers=%d  CPU=%s",
            len(targets), self.app_id_limit, max_workers, os.cpu_count(),
        )

        succeeded_ids: list[int] = []
        failed_ids: list[int] = []

        # 스레드별 독립 hook 인스턴스 (thread-local storage)
        # 각 스레드가 자신만의 DB 연결·API 세션을 가져서 conflict 방지
        thread_local = threading.local()

        def get_hooks():
            if not hasattr(thread_local, "api_hook"):
                thread_local.api_hook = SteamApiHook()
                thread_local.s3_hook  = SteamS3Hook(aws_conn_id=self.aws_conn_id)
                thread_local.pg_hook  = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            return thread_local.api_hook, thread_local.s3_hook, thread_local.pg_hook

        def collect_one(appid: int, saved_cursors: dict):
            self.log.info("리뷰 수집 시작: appid=%s  cursor=%s", appid, saved_cursors)
            api_hook, s3_hook, pg_hook = get_hooks()
            self._collect_and_store(appid, saved_cursors, api_hook, s3_hook, pg_hook)

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

    def _collect_and_store(
        self,
        appid: int,
        saved_cursors: dict,
        api_hook: SteamApiHook,
        s3_hook: SteamS3Hook,
        pg_hook: PostgresHook,
    ) -> None:
        """
        Producer-Consumer 패턴으로 리뷰를 수집하고 S3에 청크 단위로 저장.

        Producer(메인 스레드): 언어별 API 페이지 요청 → queue 적재 → rate limit 대기
        Consumer(별도 스레드): queue에서 꺼내 chunk 버퍼 누적 → CHUNK_SIZE마다 S3 저장
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
            chunk_buffer: list[dict] = []
            current_cursors: dict = dict(saved_cursors)
            chunk_idx = 0

            try:
                while True:
                    item = page_queue.get()
                    if item is None:
                        break

                    chunk_buffer.extend(item.reviews)
                    current_cursors[item.language] = item.next_cursor

                    if len(chunk_buffer) >= _CHUNK_SIZE:
                        chunk_idx += 1
                        self._upload_chunk(s3_hook, pg_hook, appid, chunk_idx, chunk_buffer, current_cursors)
                        chunk_buffer = []

                # 남은 데이터 flush
                if chunk_buffer:
                    chunk_idx += 1
                    self._upload_chunk(s3_hook, pg_hook, appid, chunk_idx, chunk_buffer, current_cursors)

                if chunk_idx == 0:
                    self.log.warning("새 리뷰 없음: appid=%s", appid)
                    # 새 리뷰가 없어도 cursor 갱신
                    self._update_cursor(pg_hook, appid, current_cursors)

            except Exception as e:
                errors.append(e)

        consumer_thread = threading.Thread(target=consumer, daemon=True)
        consumer_thread.start()
        producer()               # 메인 스레드가 producer 역할
        consumer_thread.join()

        if errors:
            raise errors[0]

    def _upload_chunk(
        self,
        s3_hook: SteamS3Hook,
        pg_hook: PostgresHook,
        appid: int,
        chunk_idx: int,
        chunk_buffer: list[dict],
        current_cursors: dict,
    ) -> None:
        """
        chunk_buffer를 Polars DataFrame으로 변환 후 Parquet으로 S3에 저장하고 cursor를 DB에 갱신.

        Steam API 응답의 author 필드는 중첩 dict인데, Polars가 struct 컬럼으로 자동 처리.
        Silver 단계에서 df["author"].struct.field("steamid") 형태로 바로 접근 가능.
        """
        df = pl.from_dicts(chunk_buffer, infer_schema_length=len(chunk_buffer))
        # appid 컬럼 추가 — Silver 조인 시 필요
        df = df.with_columns(pl.lit(appid).alias("appid"))

        s3_key = f"{self.s3_key_prefix}/{appid}_chunk_{chunk_idx:03d}.parquet"
        s3_hook.upload_parquet(df=df, key=s3_key, bucket=self.s3_bucket)
        self.log.info(
            "청크 저장 완료: appid=%s  chunk=%03d  리뷰=%d건  s3://%s/%s",
            appid, chunk_idx, len(chunk_buffer), self.s3_bucket, s3_key,
        )
        self._update_cursor(pg_hook, appid, current_cursors)

    def _fetch_targets(self) -> list[tuple[int, dict]]:
        """
        수집 대상 (appid, review_cursors) 목록 반환.

        app_ids가 지정된 경우 DB 조회 없이 해당 목록만 사용 (테스트·성능 비교용).
        미지정 시 tracked_games 테이블에서 조회.
        """
        if self.app_ids is not None:
            # cursor는 빈 값으로 — 처음부터 수집
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
        self.log.info(
            "cursor 갱신: appid=%s  cursors=%s", appid, next_cursors
        )
