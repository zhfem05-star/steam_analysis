"""
Steam API → S3 커스텀 오퍼레이터

Steam API를 호출하고 응답 JSON을 S3(MinIO)에 저장한다.
API 호출은 SteamApiHook, S3 업로드는 SteamS3Hook에 위임.
"""

from __future__ import annotations

from airflow.models import BaseOperator

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from hooks.steam_api import SteamApiHook


class SteamApiToS3Operator(BaseOperator):
    """
    Steam API를 호출한 뒤 결과를 S3에 JSON으로 저장하는 오퍼레이터.

    :param method: SteamApiHook 메서드 이름
                   ("get_app_list", "get_current_players",
                    "get_app_details", "get_app_reviews")
    :param method_params: 메서드에 전달할 keyword arguments
    :param s3_key: 저장할 S3 키 (Jinja 템플릿 사용 가능)
    :param s3_bucket: 대상 버킷 (기본값: steam-raw)
    :param aws_conn_id: Airflow S3 Connection ID
    """

    template_fields = ("s3_key", "method_params")

    def __init__(
        self,
        method: str,
        s3_key: str,
        method_params: dict | None = None,
        s3_bucket: str = BUCKET_RAW,
        aws_conn_id: str = "minio_s3",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.method = method
        self.s3_key = s3_key
        self.method_params = method_params or {}
        self.s3_bucket = s3_bucket
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        # 1) Steam API 호출
        api_hook = SteamApiHook()
        fn = getattr(api_hook, self.method, None)
        if fn is None:
            raise ValueError(f"SteamApiHook에 '{self.method}' 메서드가 없습니다")

        self.log.info("SteamApiHook.%s 호출  params=%s", self.method, self.method_params)
        data = fn(**self.method_params)

        if data is None:
            raise RuntimeError(
                f"API 응답이 None입니다: method={self.method} params={self.method_params}"
            )

        # 2) S3(MinIO)에 업로드
        try:
            s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.upload_json(data=data, key=self.s3_key, bucket=self.s3_bucket)
        except Exception:
            self.log.error(
                "S3 업로드 실패: bucket=%s  key=%s",
                self.s3_bucket,
                self.s3_key,
            )
            raise

        self.log.info("업로드 완료: s3://%s/%s", self.s3_bucket, self.s3_key)
        return self.s3_key
