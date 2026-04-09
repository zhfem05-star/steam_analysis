"""
Bronze player_counts JSON → Silver fact_concurrent_players 적재 오퍼레이터

S3 Bronze의 player_counts JSON 파일을 읽어
fact_concurrent_players 테이블에 UPSERT한다.

Bronze JSON 구조:
  [
    {"appid": 730, "player_count": 12345, "collected_at": "2025-04-09T08:00:00+00:00"},
    ...
  ]
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook


class SilverPlayersToFactOperator(BaseOperator):
    """
    Bronze player_counts JSON → fact_concurrent_players UPSERT.

    :param s3_key_prefix:    읽을 Bronze S3 prefix
                             예: "player_counts/{{ execution_date.strftime('%Y%m%d') }}"
    :param postgres_conn_id: Airflow Postgres Connection ID
    :param s3_bucket:        소스 버킷 (기본값: steam-raw)
    :param aws_conn_id:      Airflow S3 Connection ID
    """

    template_fields = ("s3_key_prefix",)

    def __init__(
        self,
        s3_key_prefix: str,
        postgres_conn_id: str = "analytics_db",
        s3_bucket: str = BUCKET_RAW,
        aws_conn_id: str = "minio_s3",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_key_prefix = s3_key_prefix
        self.postgres_conn_id = postgres_conn_id
        self.s3_bucket = s3_bucket
        self.aws_conn_id = aws_conn_id

    def execute(self, context):  # noqa: ARG002
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        keys = s3_hook.list_keys(prefix=self.s3_key_prefix, bucket=self.s3_bucket)
        json_keys = [k for k in keys if k.endswith(".json")]

        if not json_keys:
            raise ValueError(f"처리할 JSON 파일 없음: prefix={self.s3_key_prefix}")

        # 같은 날 여러 스냅샷이 있을 수 있으므로 전부 처리
        total = 0
        for key in sorted(json_keys):
            self.log.info("처리 중: %s", key)
            records = s3_hook.read_json(key=key, bucket=self.s3_bucket)

            rows = []
            for item in records:
                appid = item.get("appid")
                player_count = item.get("player_count")
                collected_at_str = item.get("collected_at")

                if appid is None or player_count is None:
                    continue

                collected_at = (
                    datetime.fromisoformat(collected_at_str)
                    if collected_at_str
                    else datetime.now(timezone.utc)
                )

                rows.append({
                    "app_id":           appid,
                    "concurrent_in_game": player_count,
                    "collected_at":     collected_at,
                })

            if rows:
                self._upsert_fact_players(pg_hook, rows)
                total += len(rows)
                self.log.info("UPSERT 완료: %d건 (%s)", len(rows), key)

        self.log.info("전체 처리 완료: %d건", total)

    # ── 내부 메서드 ──────────────────────────────────────

    def _upsert_fact_players(self, pg_hook: PostgresHook, rows: list[dict]) -> None:
        sql = """
            INSERT INTO fact_concurrent_players (
                app_id, concurrent_in_game, collected_at
            ) VALUES (
                %(app_id)s, %(concurrent_in_game)s, %(collected_at)s
            )
            ON CONFLICT (app_id, collected_at) DO UPDATE SET
                concurrent_in_game = EXCLUDED.concurrent_in_game
        """
        pg_hook.run(sql, parameters=rows)
