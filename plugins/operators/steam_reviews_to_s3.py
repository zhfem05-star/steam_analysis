"""
Steam 리뷰 수집 → S3 저장 오퍼레이터

upstream 태스크의 XCom에서 appid 리스트를 받아
각 게임의 리뷰를 수집하고 MinIO(S3)에 저장한다.
"""

from __future__ import annotations

from airflow.models import BaseOperator

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from hooks.steam_api import SteamApiHook


class SteamReviewsToS3Operator(BaseOperator):
    """
    XCom에서 appid 리스트를 받아 각 게임의 리뷰를 수집한 뒤 S3에 저장하는 오퍼레이터.

    :param app_ids_dag_id:  appid 리스트를 push한 DAG의 dag_id
    :param app_ids_task_id: appid 리스트를 push한 태스크의 task_id
    :param s3_key_prefix:   S3 저장 경로 prefix (Jinja 템플릿 사용 가능)
                            예: "reviews/{{ execution_date.strftime('%Y%m%d_%H%M') }}"
    :param num_reviews:     게임당 수집할 최대 리뷰 수 (기본값 100)
    :param s3_bucket:       대상 버킷 (기본값: steam-raw)
    :param aws_conn_id:     Airflow S3 Connection ID
    """

    template_fields = ("s3_key_prefix",)

    def __init__(
        self,
        app_ids_dag_id: str,
        app_ids_task_id: str,
        s3_key_prefix: str,
        num_reviews: int = 100,
        s3_bucket: str = BUCKET_RAW,
        aws_conn_id: str = "minio_s3",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.app_ids_dag_id = app_ids_dag_id
        self.app_ids_task_id = app_ids_task_id
        self.s3_key_prefix = s3_key_prefix
        self.num_reviews = num_reviews
        self.s3_bucket = s3_bucket
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        # 1) upstream XCom에서 appid 리스트 pull
        app_ids: list[int] = context["ti"].xcom_pull(
            dag_id=self.app_ids_dag_id,
            task_ids=self.app_ids_task_id,
        )

        if not app_ids:
            raise ValueError(
                f"XCom에서 appid 리스트를 가져오지 못했습니다. "
                f"dag_id={self.app_ids_dag_id}, task_id={self.app_ids_task_id}"
            )

        self.log.info("수집 대상 게임 수: %d", len(app_ids))

        # 2) 게임별 리뷰 수집 및 S3 저장
        api_hook = SteamApiHook()
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)

        for appid in app_ids:
            self.log.info("리뷰 수집 시작: appid=%s", appid)
            reviews = api_hook.get_app_reviews(appid=appid, num_reviews=self.num_reviews, language="korean")

            if not reviews:
                self.log.warning("리뷰 없음, 건너뜀: appid=%s", appid)
                continue

            s3_key = f"{self.s3_key_prefix}/{appid}_reviews.json"
            s3_hook.upload_json(data=reviews, key=s3_key, bucket=self.s3_bucket)
            self.log.info("업로드 완료: s3://%s/%s", self.s3_bucket, s3_key)