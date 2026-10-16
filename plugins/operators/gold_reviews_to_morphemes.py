"""
Silver 리뷰 Parquet → Gold 형태소 빈도 적재 오퍼레이터

게임별 전체 리뷰의 중복 제거 후 언어별 형태소 빈도 집계
동일 게임·언어의 기존 집계를 한 트랜잭션에서 교체

언어별 형태소 분석:
  - 한국어: kiwipiepy의 일반명사(NNG)·고유명사(NNP) 추출
  - 영어: 정규식 토큰화 후 불용어·짧은 단어 제거

Silver 경로:
  reviews/appid={appid}/language={lang}/year={Y}/month={M}/{filename}.parquet
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from kiwipiepy import Kiwi

import re
from collections import Counter
from datetime import datetime, timezone

import polars as pl
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hooks.s3_hook import BUCKET_SILVER, SteamS3Hook
from utils.review_dedup import deduplicate_reviews

_LANGUAGES = ("korean", "english")

# 영어 불용어 (NLTK stopwords 일부 + 게임 리뷰 특화 필터)
_EN_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "is",
        "it",
        "this",
        "that",
        "i",
        "you",
        "he",
        "she",
        "we",
        "they",
        "be",
        "are",
        "was",
        "were",
        "been",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "not",
        "no",
        "so",
        "if",
        "as",
        "by",
        "from",
        "up",
        "about",
        "into",
        "than",
        "then",
        "just",
        "also",
        "very",
        "more",
        "can",
        "get",
        "got",
        "its",
        "my",
        "your",
        "our",
        "their",
        "me",
        "him",
        "her",
        "us",
        "them",
        "game",
        "games",
        "play",
        "played",
        "playing",
        "player",  # 리뷰 공통 단어
    }
)

# 한국어 최소 형태소 길이 (1글자 명사 제외)
_KO_MIN_LEN = 2
# 영어 최소 단어 길이
_EN_MIN_LEN = 3
# DB VARCHAR(200) 제한에 맞춘 최대 형태소 길이
_MAX_MORPHEME_LEN = 100


class GoldReviewsToMorphemesOperator(BaseOperator):
    """
    Silver 리뷰 parquet → gold_review_morphemes UPSERT

    :param silver_prefix:    읽을 Silver S3 루트 prefix (기본값: "reviews")
    :param app_id_limit:     처리할 app_id 수 제한. None이면 전체 (테스트용)
    :param top_n:            게임·언어별 상위 N개 형태소만 저장 (기본값: 1000)
    :param postgres_conn_id: Airflow에 등록한 Postgres 연결 ID
    :param aws_conn_id:      Airflow에 등록한 S3 연결 ID
    """

    def __init__(
        self,
        silver_prefix: str = "reviews",
        app_id_limit: int | None = None,
        top_n: int = 1000,
        postgres_conn_id: str = "analytics_db",
        aws_conn_id: str = "minio_s3",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.silver_prefix = silver_prefix
        self.app_id_limit = app_id_limit
        self.top_n = top_n
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context) -> None:  # noqa: ARG002
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        app_ids = self._fetch_app_ids(pg_hook)
        if not app_ids:
            raise ValueError("tracked_games에 활성화된 app_id가 없습니다.")

        self.log.info("처리 대상 app_id: %d개", len(app_ids))

        ko_analyzer = self._get_korean_analyzer()
        total_upserted = 0

        for appid in app_ids:
            for language in _LANGUAGES:
                prefix = f"{self.silver_prefix}/appid={appid}/"
                keys = s3_hook.list_keys(prefix=prefix, bucket=BUCKET_SILVER)
                parquet_keys = [k for k in keys if k.endswith(".parquet")]

                if not parquet_keys:
                    self.log.info(
                        "파일 없음, 건너뜀: appid=%s language=%s", appid, language
                    )
                    continue

                self.log.info(
                    "형태소 분석 시작: appid=%s language=%s 파일=%d개",
                    appid,
                    language,
                    len(parquet_keys),
                )

                freq, appearances, earliest, latest = self._analyze(
                    s3_hook, parquet_keys, language, ko_analyzer
                )

                rows = self._build_rows(
                    appid, language, freq, appearances, earliest, latest
                )
                self._upsert(pg_hook, rows, appid, language)
                if rows:
                    total_upserted += len(rows)
                    self.log.info(
                        "UPSERT 완료: appid=%s language=%s 형태소=%d개",
                        appid,
                        language,
                        len(rows),
                    )

        self.log.info("전체 완료: 총 %d개 형태소 적재", total_upserted)

    # ── 분석 ─────────────────────────────────────────────

    def _analyze(
        self,
        s3_hook: SteamS3Hook,
        keys: list[str],
        language: str,
        ko_analyzer: Kiwi | None,
    ) -> tuple[Counter[str], Counter[str], datetime | None, datetime | None]:
        """
        parquet 파일들을 읽어 형태소 빈도 집계

        :return: (frequency, review_appearances, earliest_dt, latest_dt)
        """
        freq: Counter[str] = Counter()
        appearances: Counter[str] = Counter()
        earliest: datetime | None = None
        latest: datetime | None = None

        frames = [
            s3_hook.read_parquet(key=key, bucket=BUCKET_SILVER) for key in sorted(keys)
        ]
        if not frames:
            return freq, appearances, earliest, latest
        df = deduplicate_reviews(pl.concat(frames, how="diagonal_relaxed"))
        df = df.filter(pl.col("language") == language)
        if language == "korean" and ko_analyzer is None and not df.is_empty():
            raise RuntimeError(
                "한국어 분석기를 사용할 수 없어 기존 형태소 집계를 교체할 수 없습니다"
            )

        for row in df.iter_rows(named=True):
            text = row.get("review") or ""
            if not text:
                continue

            tokens = (
                self._extract_korean(text, ko_analyzer)
                if language == "korean"
                else self._extract_english(text)
            )
            if not tokens:
                continue

            freq.update(tokens)
            appearances.update(set(tokens))  # 리뷰당 1회만 카운트

            # 시간 범위 추적
            ts = row.get("timestamp_created")
            if ts is not None:
                dt = (
                    ts
                    if isinstance(ts, datetime)
                    else datetime.fromtimestamp(ts, tz=timezone.utc)
                )
                if earliest is None or dt < earliest:
                    earliest = dt
                if latest is None or dt > latest:
                    latest = dt

        return freq, appearances, earliest, latest

    def _build_rows(
        self,
        appid: int,
        language: str,
        freq: Counter[str],
        appearances: Counter[str],
        earliest: datetime | None,
        latest: datetime | None,
    ) -> list[dict[str, Any]]:
        """
        상위 top_n 형태소만 추려 DB 삽입용 dict 리스트 반환
        """
        now = datetime.now(timezone.utc)
        return [
            {
                "app_id": appid,
                "language": language,
                "morpheme": morpheme,
                "frequency": count,
                "review_appearances": appearances[morpheme],
                "earliest_review_at": earliest,
                "latest_review_at": latest,
                "updated_at": now,
            }
            for morpheme, count in freq.most_common(self.top_n)
        ]

    # ── 형태소 추출 ──────────────────────────────────────

    @staticmethod
    def _get_korean_analyzer() -> Kiwi | None:
        """
        kiwipiepy Kiwi 인스턴스 반환. import 실패 시 None
        """
        try:
            from kiwipiepy import Kiwi

            return Kiwi()
        except ImportError:
            return None

    @staticmethod
    def _extract_korean(text: str, analyzer: Kiwi | None) -> list[str]:
        """
        kiwipiepy로 한국어 명사(NNG, NNP) 추출
        analyzer가 None이면 빈 리스트 반환
        """
        if analyzer is None:
            return []
        tokens = analyzer.tokenize(text)
        return [
            token.form
            for token in tokens
            if token.tag in ("NNG", "NNP")
            and _KO_MIN_LEN <= len(token.form) <= _MAX_MORPHEME_LEN
        ]

    @staticmethod
    def _extract_english(text: str) -> list[str]:
        """
        NLTK 없이 정규식 기반 영어 토크나이징
        소문자 변환 → 알파벳만 추출 → 불용어·짧은 단어 제거
        """
        words = re.findall(r"[a-zA-Z]+", text.lower())
        return [
            w
            for w in words
            if _EN_MIN_LEN <= len(w) <= _MAX_MORPHEME_LEN and w not in _EN_STOPWORDS
        ]

    # ── DB ───────────────────────────────────────────────

    def _fetch_app_ids(self, pg_hook: PostgresHook) -> list[int]:
        if self.app_id_limit:
            sql = """
                SELECT appid FROM tracked_games
                WHERE is_active = TRUE ORDER BY appid LIMIT %s
            """
            rows = pg_hook.get_records(sql, parameters=(self.app_id_limit,))
        else:
            sql = """
                SELECT appid FROM tracked_games
                WHERE is_active = TRUE ORDER BY appid
            """
            rows = pg_hook.get_records(sql, parameters=None)
        return [r[0] for r in rows]

    def _upsert(
        self,
        pg_hook: PostgresHook,
        rows: list[dict[str, Any]],
        appid: int,
        language: str,
    ) -> None:
        sql = """
            INSERT INTO gold_review_morphemes (
                app_id, language, morpheme, frequency,
                review_appearances, earliest_review_at, latest_review_at, updated_at
            ) VALUES (
                %(app_id)s, %(language)s, %(morpheme)s, %(frequency)s,
                %(review_appearances)s, %(earliest_review_at)s,
                %(latest_review_at)s, %(updated_at)s
            )
            ON CONFLICT (app_id, language, morpheme) DO UPDATE SET
                frequency           = EXCLUDED.frequency,
                review_appearances  = EXCLUDED.review_appearances,
                earliest_review_at  = EXCLUDED.earliest_review_at,
                latest_review_at    = EXCLUDED.latest_review_at,
                updated_at          = EXCLUDED.updated_at
        """
        conn = pg_hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM gold_review_morphemes WHERE app_id = %s AND language = %s",
                    (appid, language),
                )
                if rows:
                    cur.executemany(sql, rows)
