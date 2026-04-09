"""
Steam 프로젝트 전용 S3 Hook

MinIO(로컬)와 AWS S3(운영) 모두 동일한 인터페이스로 사용 가능.
aws_conn_id만 변경하면 환경 전환 완료.
"""

import io
import json

import polars as pl
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET_RAW = "steam-raw"
BUCKET_PROCESSED = "steam-processed"


class SteamS3Hook:
    """
    S3/MinIO 업로드·다운로드를 위한 프로젝트 전용 Hook
    """

    def __init__(self, aws_conn_id: str = "minio_s3"):
        """
        :param aws_conn_id: Airflow에 설정된 S3 연결 ID
        :return: None
        """
        self._hook = S3Hook(aws_conn_id=aws_conn_id)


    def upload_json(
        self,
        data: dict | list,
        key: str,
        bucket: str = BUCKET_RAW,
    ) -> None:
        """
        Python 객체를 JSON으로 직렬화하여 S3에 업로드
        key는 경로. 예: folder/subfolder/file.json
        bucket은 S3 버킷 이름. 스키마나 DB 이름 같은 것
        """
        self._hook.load_string(
            string_data=json.dumps(data, ensure_ascii=False, indent=2),
            key=key,
            bucket_name=bucket,
            replace=True,
        )

    def upload_string(
        self,
        data: str,
        key: str,
        bucket: str = BUCKET_RAW,
    ) -> None:
        """문자열 데이터를 그대로 S3에 업로드"""
        self._hook.load_string(
            string_data=data,
            key=key,
            bucket_name=bucket,
            replace=True,
        )

    def read_json(self, key: str, bucket: str = BUCKET_RAW) -> dict | list:
        """S3에서 JSON 파일을 읽어 Python 객체로 반환"""
        content = self._hook.read_key(key=key, bucket_name=bucket)
        return json.loads(content)

    def upload_parquet(
        self,
        df: pl.DataFrame,
        key: str,
        bucket: str = BUCKET_RAW,
        compression: str = "snappy",
    ) -> None:
        """
        Polars DataFrame을 Parquet으로 직렬화하여 S3에 업로드.

        :param compression: "snappy"(기본, 빠름) | "zstd"(압축률 높음) | "gzip"
        """
        buf = io.BytesIO()
        df.write_parquet(buf, compression=compression)
        self._hook.load_bytes(
            bytes_data=buf.getvalue(),
            key=key,
            bucket_name=bucket,
            replace=True,
        )

    def read_parquet(self, key: str, bucket: str = BUCKET_RAW) -> pl.DataFrame:
        """S3에서 Parquet 파일을 읽어 Polars DataFrame으로 반환."""
        content = self._hook.read_key(key=key, bucket_name=bucket)
        return pl.read_parquet(io.BytesIO(content.encode() if isinstance(content, str) else content))

    def key_exists(self, key: str, bucket: str = BUCKET_RAW) -> bool:
        """S3 키 존재 여부 확인"""
        return self._hook.check_for_key(key=key, bucket_name=bucket)
