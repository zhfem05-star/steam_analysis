"""
Bronze reviews parquet → Silver S3 파티셔닝 오퍼레이터

S3 Bronze의 리뷰 parquet 청크 파일들을 읽어 변환한 뒤
Silver 버킷에 appid / language / year / month 기준으로 파티셔닝하여 저장한다.

Bronze 구조:  reviews/{YYYYMMDD_HHMM}/{appid}_chunk_{N:03d}.parquet
Silver 구조:  reviews/appid={appid}/language={lang}/year={Y}/month={M}/{filename}.parquet

파티션 키에 language를 포함하므로 경로만으로 언어 구분 가능:
  - reviews/appid=730/language=korean/...
  - reviews/appid=730/language=english/...

변환 내용:
  - author struct → 컬럼으로 펼치기 (author_steamid 등)
  - timestamp_created / timestamp_updated → UTC datetime 변환
  - review_year, review_month 파티션 컬럼 추가 (timestamp_created 기준)
"""

from __future__ import annotations

import polars as pl
from airflow.models import BaseOperator

from hooks.s3_hook import BUCKET_RAW, BUCKET_SILVER, SteamS3Hook


class SilverReviewsToS3Operator(BaseOperator):
    """
    Bronze 리뷰 parquet → Silver S3 파티셔닝 저장.

    :param bronze_prefix:  읽을 Bronze S3 prefix
                           예: "reviews/{{ execution_date.strftime('%Y%m%d') }}"
    :param silver_prefix:  Silver 저장 루트 prefix (기본값: "reviews")
    :param aws_conn_id:    Airflow S3 Connection ID
    """

    template_fields = ("bronze_prefix",)

    def __init__(
        self,
        bronze_prefix: str,
        silver_prefix: str = "reviews",
        aws_conn_id: str = "minio_s3",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bronze_prefix = bronze_prefix
        self.silver_prefix = silver_prefix
        self.aws_conn_id = aws_conn_id

    def execute(self, context):  # noqa: ARG002
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)

        keys = s3_hook.list_keys(prefix=self.bronze_prefix, bucket=BUCKET_RAW)
        parquet_keys = [k for k in keys if k.endswith(".parquet")]

        if not parquet_keys:
            raise ValueError(f"처리할 parquet 파일 없음: prefix={self.bronze_prefix}")

        self.log.info("처리 대상 파일 수: %d", len(parquet_keys))

        processed = 0
        for key in parquet_keys:
            df = s3_hook.read_parquet(key=key, bucket=BUCKET_RAW)
            df = self._transform(df)

            filename = key.split("/")[-1]  # {appid}_chunk_{N:03d}.parquet

            # appid + language + year + month 로 그룹화하여 파티션 경로에 저장
            for (appid_val, lang, year, month), group_df in df.group_by(
                ["appid", "language", "review_year", "review_month"]
            ):
                silver_key = (
                    f"{self.silver_prefix}/"
                    f"appid={appid_val}/"
                    f"language={lang}/"
                    f"year={year}/month={month:02d}/"
                    f"{filename}"
                )

                s3_hook.upload_parquet(df=group_df, key=silver_key, bucket=BUCKET_SILVER)
                self.log.info(
                    "Silver 저장: appid=%s  language=%s  %d/%02d  리뷰=%d건",
                    appid_val, lang, year, month, len(group_df),
                )

            processed += 1

        self.log.info("처리 완료: %d개 파일", processed)

    # ── 내부 메서드 ──────────────────────────────────────

    @staticmethod
    def _transform(df: pl.DataFrame) -> pl.DataFrame:
        """
        Bronze 리뷰 DataFrame 변환.

        1. author struct → 개별 컬럼으로 펼치기
        2. Unix timestamp → UTC datetime
        3. 파티션 컬럼 (review_year, review_month) 추가
        """
        # author struct 펼치기
        if "author" in df.columns:
            author_df = df.select(pl.col("author").struct.unnest())
            author_df = author_df.rename(
                {col: f"author_{col}" for col in author_df.columns}
            )
            df = pl.concat([df.drop("author"), author_df], how="horizontal")

        # Unix timestamp → datetime (초 단위)
        for col in ("timestamp_created", "timestamp_updated"):
            if col in df.columns:
                df = df.with_columns(
                    pl.from_epoch(pl.col(col), time_unit="s")
                    .dt.replace_time_zone("UTC")
                    .alias(col)
                )

        # 파티션 컬럼: timestamp_created 기준 년·월
        if "timestamp_created" in df.columns:
            df = df.with_columns([
                pl.col("timestamp_created").dt.year().alias("review_year"),
                pl.col("timestamp_created").dt.month().alias("review_month"),
            ])
        else:
            df = df.with_columns([
                pl.lit(0).alias("review_year"),
                pl.lit(0).alias("review_month"),
            ])

        return df
