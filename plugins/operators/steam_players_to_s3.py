"""
Steam 동접자 수 수집 → S3 저장 오퍼레이터

tracked_games 컨트롤 테이블에서 is_active=TRUE, collect_players=TRUE인
app_id 목록을 조회하여 동접자 수를 수집하고 MinIO(S3)에 저장한다.
"""

from __future__ import annotations

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from hooks.steam_api import SteamApiHook


class SteamPlayersToS3Operator(BaseOperator):
    """
    tracked_games 테이블에서 app_id를 조회하여 동접자 수를 수집 후 S3에 저장.

    :param s3_key:           S3 저장 경로 (Jinja 템플릿 사용 가능)
    :param postgres_conn_id: Airflow Postgres Connection ID
    :param s3_bucket:        대상 버킷 (기본값: steam-raw)
    :param aws_conn_id:      Airflow S3 Connection ID
    """

    template_fields = ("s3_key",)

    def __init__(
        self,
        s3_key: str,
        postgres_conn_id: str = "analytics_db",
        s3_bucket: str = BUCKET_RAW,
        aws_conn_id: str = "minio_s3",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.s3_bucket = s3_bucket
        self.aws_conn_id = aws_conn_id

    def execute(self, context):  # noqa: ARG002
        # 1) tracked_games에서 수집 대상 app_id 조회
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        rows = pg_hook.get_records(
            """
            SELECT appid FROM tracked_games
            WHERE is_active = TRUE AND collect_players = TRUE
            """
        )
        app_ids = [row[0] for row in rows]

        if not app_ids:
            raise ValueError("tracked_games 테이블에서 동접자 수집 대상 app_id를 가져오지 못했습니다.")

        self.log.info("동접자 수집 대상 게임 수: %d", len(app_ids))

        # 2) 게임별 동접자 수 수집
        api_hook = SteamApiHook()
        results = []

        for appid in app_ids:
            player_count = api_hook.get_current_players(appid=appid)

            if player_count is None:
                self.log.warning("동접자 조회 실패, 건너뜀: appid=%s", appid)
                continue

            results.append({
                "appid": appid,
                "player_count": player_count,
                "collected_at": timezone.utcnow().isoformat(),
            })
            self.log.info("동접자 수집 완료: appid=%s, count=%d", appid, player_count)

        # 3) S3에 저장
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.upload_json(data=results, key=self.s3_key, bucket=self.s3_bucket)
        self.log.info("업로드 완료: s3://%s/%s (%d개)", self.s3_bucket, self.s3_key, len(results))
