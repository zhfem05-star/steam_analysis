"""
Bronze appdetails → Silver dim 테이블 적재 오퍼레이터

S3 Bronze의 appdetails parquet 파일을 읽어
dim_games, dim_genres, dim_game_genres 테이블에 UPSERT한다.

처리 흐름:
  S3 bronze: appdetails/{prefix}/{appid}.parquet
    → Polars 변환 (release_date 파싱, genres 분리)
    → dim_games UPSERT
    → dim_genres UPSERT (신규 장르만)
    → dim_game_genres UPSERT
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook

# release_date 파싱에 사용할 포맷 후보 (언어 설정에 따라 형식이 달라질 수 있음)
_DATE_FORMATS = ["%b %d, %Y", "%d %b, %Y", "%Y-%m-%d"]


def _parse_release_date(date_str: str) -> date | None:
    """'Jul 9, 2013' 형태 문자열을 date 객체로 변환. 실패 시 None."""
    if not date_str:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


class SilverAppDetailsToDimOperator(BaseOperator):
    """
    Bronze appdetails parquet → dim_games / dim_genres / dim_game_genres UPSERT.

    :param s3_key_prefix:    읽을 Bronze S3 prefix
                             예: "appdetails/{{ execution_date.strftime('%Y%m%d_%H%M') }}"
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
        parquet_keys = [k for k in keys if k.endswith(".parquet")]

        if not parquet_keys:
            raise ValueError(f"처리할 parquet 파일 없음: prefix={self.s3_key_prefix}")

        self.log.info("처리 대상 파일 수: %d", len(parquet_keys))

        game_rows: list[dict] = []
        genre_map: dict[int, str] = {}      # genre_id → genre_name (중복 제거)
        game_genre_pairs: list[tuple] = []  # (appid, genre_id)

        for key in parquet_keys:
            df = s3_hook.read_parquet(key=key, bucket=self.s3_bucket)
            for row in df.to_dicts():
                appid = row["appid"]

                # ── dim_games 행 구성 ──
                release_date = _parse_release_date(row.get("release_date_str", ""))
                game_rows.append({
                    "app_id":             appid,
                    "name":               row.get("name", ""),
                    "type":               row.get("type", ""),
                    "is_free":            row.get("is_free", False),
                    "required_age":       row.get("required_age", 0),
                    "developers":         row.get("developers", ""),
                    "publishers":         row.get("publishers", ""),
                    "release_date":       release_date,
                    "platforms_windows":  row.get("platforms_windows", False),
                    "platforms_mac":      row.get("platforms_mac", False),
                    "platforms_linux":    row.get("platforms_linux", False),
                    "header_image_url":   row.get("header_image", ""),
                    "total_recommendations": row.get("recommendations_total"),
                })

                # ── dim_genres / dim_game_genres 행 구성 ──
                genres = row.get("genres") or []
                for g in genres:
                    gid = g.get("genre_id")
                    gname = g.get("genre_name", "")
                    if gid is not None:
                        genre_map[gid] = gname
                        game_genre_pairs.append((appid, gid))

        self.log.info(
            "수집 완료: 게임=%d  장르=%d  게임-장르 관계=%d",
            len(game_rows), len(genre_map), len(game_genre_pairs),
        )

        self._upsert_dim_games(pg_hook, game_rows)
        self._upsert_dim_genres(pg_hook, genre_map)
        self._upsert_dim_game_genres(pg_hook, game_genre_pairs)

    # ── 내부 메서드 ──────────────────────────────────────

    def _upsert_dim_games(self, pg_hook: PostgresHook, rows: list[dict]) -> None:
        sql = """
            INSERT INTO dim_games (
                app_id, name, type, is_free, required_age,
                developers, publishers, release_date,
                platforms_windows, platforms_mac, platforms_linux,
                header_image_url, total_recommendations, updated_at
            ) VALUES (
                %(app_id)s, %(name)s, %(type)s, %(is_free)s, %(required_age)s,
                %(developers)s, %(publishers)s, %(release_date)s,
                %(platforms_windows)s, %(platforms_mac)s, %(platforms_linux)s,
                %(header_image_url)s, %(total_recommendations)s, NOW()
            )
            ON CONFLICT (app_id) DO UPDATE SET
                name                  = EXCLUDED.name,
                type                  = EXCLUDED.type,
                is_free               = EXCLUDED.is_free,
                required_age          = EXCLUDED.required_age,
                developers            = EXCLUDED.developers,
                publishers            = EXCLUDED.publishers,
                release_date          = EXCLUDED.release_date,
                platforms_windows     = EXCLUDED.platforms_windows,
                platforms_mac         = EXCLUDED.platforms_mac,
                platforms_linux       = EXCLUDED.platforms_linux,
                header_image_url      = EXCLUDED.header_image_url,
                total_recommendations = EXCLUDED.total_recommendations,
                updated_at            = NOW()
        """
        pg_hook.run(sql, parameters=rows)
        self.log.info("dim_games UPSERT 완료: %d건", len(rows))

    def _upsert_dim_genres(self, pg_hook: PostgresHook, genre_map: dict[int, str]) -> None:
        if not genre_map:
            return
        sql = """
            INSERT INTO dim_genres (genre_id, genre_name)
            VALUES (%s, %s)
            ON CONFLICT (genre_id) DO UPDATE SET
                genre_name = EXCLUDED.genre_name
        """
        pg_hook.run(sql, parameters=list(genre_map.items()))
        self.log.info("dim_genres UPSERT 완료: %d건", len(genre_map))

    def _upsert_dim_game_genres(
        self, pg_hook: PostgresHook, pairs: list[tuple]
    ) -> None:
        if not pairs:
            return
        sql = """
            INSERT INTO dim_game_genres (app_id, genre_id)
            VALUES (%s, %s)
            ON CONFLICT (app_id, genre_id) DO NOTHING
        """
        pg_hook.run(sql, parameters=pairs)
        self.log.info("dim_game_genres UPSERT 완료: %d건", len(pairs))
