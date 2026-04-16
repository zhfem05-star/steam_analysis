"""
Silver S3 reviews → fact_review_daily 일별 집계 오퍼레이터

Silver S3의 리뷰 parquet을 app_id별로 읽어
game / language / review_date 기준으로 일별 리뷰 수를 집계한 뒤
fact_review_daily 테이블에 UPSERT한다.

A/B 테스트 활용 예시:
  SELECT
      r.app_id,
      r.review_date,
      r.review_count,
      p.is_discounted,
      p.discount_percent
  FROM fact_review_daily r
  LEFT JOIN fact_price_history p
         ON r.app_id     = p.app_id
        AND r.review_date = p.collected_at
  WHERE r.app_id = 730

Silver S3 구조:
  reviews/appid={appid}/language={lang}/year={Y}/month={M}/{filename}.parquet
"""

from __future__ import annotations

import polars as pl
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from hooks.s3_hook import BUCKET_SILVER, SteamS3Hook


class SilverReviewDailyToFactOperator(BaseOperator):
    """
    Silver S3 리뷰 parquet → fact_review_daily UPSERT.

    tracked_games 테이블에서 is_active=TRUE인 app_id를 조회하여
    Silver S3에서 해당 게임의 리뷰를 읽고 일별 집계 후 적재한다.

    :param postgres_conn_id: Airflow Postgres Connection ID
    :param aws_conn_id:      Airflow S3 Connection ID
    :param silver_prefix:    Silver S3 리뷰 루트 prefix (기본값: "reviews")
    """

    def __init__(
        self,
        postgres_conn_id: str = "analytics_db",
        aws_conn_id: str = "minio_s3",
        silver_prefix: str = "reviews",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id
        self.silver_prefix = silver_prefix

    def execute(self, context):  # noqa: ARG002
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        app_ids = self._get_tracked_app_ids(pg_hook)
        self.log.info("처리 대상 게임: %d개", len(app_ids))

        total_rows = 0
        for app_id in app_ids:
            prefix = f"{self.silver_prefix}/appid={app_id}/"
            keys = s3_hook.list_keys(prefix=prefix, bucket=BUCKET_SILVER)
            parquet_keys = [k for k in keys if k.endswith(".parquet")]

            if not parquet_keys:
                self.log.debug("Silver 리뷰 없음: appid=%s", app_id)
                continue

            dfs = [
                s3_hook.read_parquet(key=k, bucket=BUCKET_SILVER)
                for k in parquet_keys
            ]
            df = pl.concat(dfs, how="diagonal")

            rows = self._aggregate_daily(df, app_id)
            if not rows:
                continue

            self._upsert(pg_hook, rows)
            total_rows += len(rows)
            self.log.info("appid=%s: %d개 날짜 적재", app_id, len(rows))

        self.log.info("전체 완료: %d건", total_rows)

    # ── 내부 메서드 ──────────────────────────────────────

    @staticmethod
    def _get_tracked_app_ids(pg_hook: PostgresHook) -> list[int]:
        rows = pg_hook.get_records(
            "SELECT appid FROM tracked_games WHERE is_active = TRUE"
        )
        return [r[0] for r in rows]

    @staticmethod
    def _aggregate_daily(df: pl.DataFrame, app_id: int) -> list[dict]:
        """
        리뷰 DataFrame을 언어·날짜 기준으로 집계.

        timestamp_created: Silver에서 이미 UTC datetime으로 변환되어 있음.
        language: Silver parquet 컬럼에 포함되어 있음.
        """
        if "timestamp_created" not in df.columns or "language" not in df.columns:
            return []

        # Silver에서 Unix int로 저장된 경우 방어적 변환
        if df["timestamp_created"].dtype in (pl.Int32, pl.Int64):
            df = df.with_columns(
                pl.from_epoch(pl.col("timestamp_created"), time_unit="s")
                .dt.replace_time_zone("UTC")
                .alias("timestamp_created")
            )

        agg = (
            df
            .with_columns(
                pl.col("timestamp_created").dt.date().alias("review_date")
            )
            .group_by(["language", "review_date"])
            .agg(pl.len().alias("review_count"))
        )

        return [
            {
                "app_id":       app_id,
                "language":     row["language"],
                "review_date":  row["review_date"],
                "review_count": row["review_count"],
            }
            for row in agg.to_dicts()
        ]

    def _upsert(self, pg_hook: PostgresHook, rows: list[dict]) -> None:
        sql = """
            INSERT INTO fact_review_daily (
                app_id, language, review_date, review_count
            ) VALUES (
                %(app_id)s, %(language)s, %(review_date)s, %(review_count)s
            )
            ON CONFLICT (app_id, language, review_date) DO UPDATE SET
                review_count = EXCLUDED.review_count,
                updated_at   = NOW()
        """
        conn = pg_hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
