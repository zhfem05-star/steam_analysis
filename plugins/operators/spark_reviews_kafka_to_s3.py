"""
Kafka(steam-reviews) → Spark Structured Streaming → MinIO Bronze(steam-raw) 저장 오퍼레이터

기존 SteamReviewsToS3Operator가 queue.Queue(인메모리)로 연결하던
producer-consumer 구조를, 실제 MQ인 Kafka로 대체한다.
SteamReviewsToKafkaOperator(producer)가 Kafka로 발행한 리뷰를 이 오퍼레이터가
소비하여, 기존과 완전히 동일한 Bronze S3 구조로 저장한다.

저장 경로: reviews/{s3_key_prefix}/{appid}_chunk_{N:03d}.parquet
(SilverReviewsToS3Operator 등 하위 파이프라인이 이 구조를 그대로 기대하므로
 저장 방식은 절대 바꾸지 않는다.)

Kafka 읽기는 Spark(분산)로 하되, 실제 S3 저장은 기존 SteamS3Hook(boto3/polars)을
그대로 재사용한다 — Hadoop S3A 커넥터 없이도 기존과 동일한 저장 포맷을 보장하기 위함.

trigger(availableNow=True)로 동작 — 실행 시점까지 Kafka에 쌓인 메시지를 전부
처리하고 종료한다 (상시 스트리밍 서비스가 아니라 Airflow task 생명주기에 맞춤).
"""

from __future__ import annotations

import json

import polars as pl
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from hooks.spark_hook import SteamSparkHook

# 클러스터 Spark 버전(4.2.0)·Scala 버전(2.13)과 반드시 일치해야 함
_KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


class SparkReviewsKafkaToS3Operator(BaseOperator):
    """
    Kafka steam-reviews 토픽을 소비하여 appid별로 Bronze S3에 Parquet 청크 저장.

    :param s3_key_prefix:    S3 저장 경로 prefix (Jinja 템플릿 사용 가능)
                             예: "reviews/{{ execution_date.strftime('%Y%m%d_%H%M') }}"
    :param kafka_topic:      구독할 Kafka 토픽명
    :param kafka_conn_id:    Airflow Kafka Connection ID
    :param spark_conn_id:    Airflow Spark Connection ID
    :param aws_conn_id:      Airflow S3 Connection ID
    :param s3_bucket:        대상 버킷 (기본값: steam-raw)
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
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_key_prefix = s3_key_prefix
        self.kafka_topic = kafka_topic
        self.kafka_conn_id = kafka_conn_id
        self.spark_conn_id = spark_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.checkpoint_path = checkpoint_path

    def execute(self, context):  # noqa: ARG002
        spark = SteamSparkHook(spark_conn_id=self.spark_conn_id).get_session(
            packages=[_KAFKA_CONNECTOR_PACKAGE]
        )

        kafka_conn = BaseHook.get_connection(self.kafka_conn_id)
        bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

        raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        messages = raw.select(col("value").cast("string").alias("review_json"))

        def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
            rows = batch_df.collect()
            if not rows:
                self.log.info("배치 %d: 새 메시지 없음", batch_id)
                return

            records = [json.loads(r["review_json"]) for r in rows]

            # appid별로 묶어서 기존과 동일하게 파일 하나씩 생성
            by_appid: dict[int, list[dict]] = {}
            for rec in records:
                by_appid.setdefault(rec["appid"], []).append(rec)

            s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)
            for appid, recs in by_appid.items():
                df = pl.DataFrame(recs, infer_schema_length=len(recs))
                chunk_key = f"{self.s3_key_prefix}/{appid}_chunk_{batch_id:03d}.parquet"
                s3_hook.upload_parquet(df=df, key=chunk_key, bucket=self.s3_bucket)
                self.log.info(
                    "배치 %d: appid=%s  %d건 저장 → s3://%s/%s",
                    batch_id, appid, len(recs), self.s3_bucket, chunk_key,
                )

        query = (
            messages.writeStream
            .foreachBatch(_write_batch)
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        spark.stop()
