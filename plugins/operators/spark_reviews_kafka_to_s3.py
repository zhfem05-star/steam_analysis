"""
Kafka 리뷰 → Spark Structured Streaming → Bronze S3 저장 오퍼레이터

SteamReviewsToKafkaOperator에서 발행한 메시지를 게임별 Parquet 청크로 저장
Spark에서 Kafka 메시지 조회 후 SteamS3Hook으로 S3 업로드
availableNow 트리거로 실행 시점까지 쌓인 메시지 처리 후 종료

저장 경로:
  reviews/{s3_key_prefix}/{appid}_chunk_{N:03d}.parquet
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.utils.context import Context

import json

import polars as pl
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from hooks.spark_hook import SteamSparkHook
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# 클러스터 Spark 버전(4.2.0)·Scala 버전(2.13)과 반드시 일치해야 함
_KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


class SparkReviewsKafkaToS3Operator(BaseOperator):
    """
    Kafka steam-reviews 토픽을 소비하여 appid별로 Bronze S3에 Parquet 청크 저장

    :param s3_key_prefix:    S3 저장 경로 prefix (Jinja 템플릿 사용 가능)
                             예: "reviews/{{ execution_date.strftime('%Y%m%d_%H%M') }}"
    :param kafka_topic:      구독할 Kafka 토픽명
    :param kafka_conn_id:    Airflow에 등록한 Kafka 연결 ID
    :param spark_conn_id:    Airflow에 등록한 Spark 연결 ID
    :param aws_conn_id:      Airflow에 등록한 S3 연결 ID
    :param s3_bucket:        대상 버킷 (기본값: steam-raw)
    :param max_offsets_per_trigger: 한 배치의 최대 메시지 수 (driver 메모리 사용량 제한)
    :param checkpoint_path:  Structured Streaming checkpoint 경로
                              (driver·executor가 공유하는 볼륨 경로여야 함)
    """

    template_fields = ("s3_key_prefix",)

    def __init__(
        self,
        s3_key_prefix: str,
        kafka_topic: str = "steam-reviews",
        kafka_conn_id: str = "kafka_default",
        spark_conn_id: str = "spark_default",
        aws_conn_id: str = "minio_s3",
        s3_bucket: str = BUCKET_RAW,
        checkpoint_path: str = "/opt/spark/checkpoints/reviews_kafka_to_s3",
        max_offsets_per_trigger: int = 10000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_key_prefix = s3_key_prefix
        self.kafka_topic = kafka_topic
        self.kafka_conn_id = kafka_conn_id
        self.spark_conn_id = spark_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.checkpoint_path = checkpoint_path
        if type(max_offsets_per_trigger) is not int or max_offsets_per_trigger < 1:
            raise ValueError("max_offsets_per_trigger는 1 이상의 정수여야 합니다")
        self.max_offsets_per_trigger = max_offsets_per_trigger

    def execute(self, context: Context) -> None:  # noqa: ARG002
        spark = SteamSparkHook(spark_conn_id=self.spark_conn_id).get_session(
            packages=[_KAFKA_CONNECTOR_PACKAGE]
        )

        kafka_conn = BaseHook.get_connection(self.kafka_conn_id)
        bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

        # Kafka 메시지 스트림 구성
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", self.max_offsets_per_trigger)
            .load()
        )

        messages = raw.select(col("value").cast("string").alias("review_json"))

        # 배치별 메시지를 게임 단위로 묶어 Bronze에 저장
        def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
            rows = batch_df.collect()
            if not rows:
                self.log.info("배치 %d: 새 메시지 없음", batch_id)
                return

            records = [json.loads(r["review_json"]) for r in rows]

            # appid별로 묶어서 기존과 동일하게 파일 하나씩 생성
            by_appid: dict[int, list[dict[str, Any]]] = {}
            for rec in records:
                by_appid.setdefault(rec["appid"], []).append(rec)

            s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)
            for appid, recs in by_appid.items():
                df = pl.DataFrame(recs, infer_schema_length=len(recs))
                chunk_key = f"{self.s3_key_prefix}/{appid}_chunk_{batch_id:03d}.parquet"
                s3_hook.upload_parquet(df=df, key=chunk_key, bucket=self.s3_bucket)
                self.log.info(
                    "배치 %d: appid=%s  %d건 저장 → s3://%s/%s",
                    batch_id,
                    appid,
                    len(recs),
                    self.s3_bucket,
                    chunk_key,
                )

        # 실행 시점까지 쌓인 메시지 처리 및 종료 대기
        query = (
            messages.writeStream.foreachBatch(_write_batch)
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        spark.stop()
