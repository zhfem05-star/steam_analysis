"""
Steam 앱 상세 정보 수집 → S3 저장 오퍼레이터

tracked_games 컨트롤 테이블의 is_active=TRUE인 app_id를 조회하여
appdetails API에서 게임 정보(이름, 장르, 플랫폼 등)를 수집하고
MinIO(S3) steam-raw 버킷에 Parquet 형태로 저장한다.

저장 경로: {s3_key_prefix}/{appid}.parquet
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import polars as pl

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from callbacks.slack_callback import slack_collect_summary
from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from hooks.steam_api import SteamApiHook

# appdetails rate limit: 200 req/5min → 1.5초 간격
_RATE_LIMIT_SEC = 1.5


class SteamAppDetailsToS3Operator(BaseOperator):
    """
    tracked_games 테이블의 app_id를 기준으로 appdetails API를 호출하여
    게임 메타데이터(이름·장르·플랫폼·개발사 등)를 Parquet으로 S3에 저장.

    :param s3_key_prefix:    S3 저장 경로 prefix (Jinja 템플릿 사용 가능)
                             예: "appdetails/{{ execution_date.strftime('%Y%m%d_%H%M') }}"
    :param app_id_limit:     조회할 app_id 수 제한. None이면 전체 조회 (테스트 시 소수 지정)
    :param cc:               국가 코드 (가격·등급 정보에 영향). 기본값 "kr"
    :param language:         언어 설정. 기본값 "korean"
    :param postgres_conn_id: Airflow Postgres Connection ID
    :param s3_bucket:        대상 버킷 (기본값: steam-raw)
    :param aws_conn_id:      Airflow S3 Connection ID
    """

    template_fields = ("s3_key_prefix",)

    def __init__(
        self,
        s3_key_prefix: str,
        app_id_limit: int | None = None,
        cc: str = "kr",
        language: str = "korean",
        postgres_conn_id: str = "analytics_db",
        s3_bucket: str = BUCKET_RAW,
        aws_conn_id: str = "minio_s3",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_key_prefix = s3_key_prefix
        self.app_id_limit = app_id_limit
        self.cc = cc
        self.language = language
        self.postgres_conn_id = postgres_conn_id
        self.s3_bucket = s3_bucket
        self.aws_conn_id = aws_conn_id

    @staticmethod
    def _determine_workers(target_count: int) -> int:
        """
        환경에 맞는 worker 수를 동적으로 결정.

        appdetails는 appid당 요청 1회로 끝나는 단순 I/O 작업.
        rate limit(200 req/5min)을 고려해 최대 4개로 제한.
        실제 대상 수보다 많은 worker는 의미 없으므로 target_count로 상한.
        """
        cpu_count = os.cpu_count() or 1
        ideal = cpu_count + 4
        return min(ideal, 4, target_count)

    def execute(self, context):  # noqa: ARG002
        app_ids = self._fetch_app_ids()

        if not app_ids:
            raise ValueError("tracked_games 테이블에서 수집 대상 app_id를 가져오지 못했습니다.")

        max_workers = self._determine_workers(len(app_ids))
        self.log.info(
            "수집 대상: %d개 게임 (limit=%s)  workers=%d",
            len(app_ids), self.app_id_limit, max_workers,
        )

        succeeded_ids: list[int] = []
        failed_ids: list[int] = []

        api_hook = SteamApiHook()
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)

        def collect_one(appid: int):
            detail = api_hook.get_app_details(appid=appid, cc=self.cc, language=self.language)
            time.sleep(_RATE_LIMIT_SEC)

            if detail is None:
                self.log.warning("앱 상세 조회 실패, 건너뜀: appid=%s", appid)
                return False

            row = self._extract_fields(appid, detail)
            df = pl.DataFrame([row])
            s3_key = f"{self.s3_key_prefix}/{appid}.parquet"
            s3_hook.upload_parquet(df=df, key=s3_key, bucket=self.s3_bucket)
            self.log.info("저장 완료: appid=%s  s3://%s/%s", appid, self.s3_bucket, s3_key)
            return True

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_appid = {executor.submit(collect_one, appid): appid for appid in app_ids}
            for future in as_completed(future_to_appid):
                appid = future_to_appid[future]
                try:
                    ok = future.result()
                    if ok:
                        succeeded_ids.append(appid)
                    else:
                        failed_ids.append(appid)
                except Exception as e:
                    self.log.error("appid=%s 수집 실패: %s", appid, e)
                    failed_ids.append(appid)

        ti = context["ti"]
        slack_collect_summary(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            log_url=ti.log_url,
            succeeded=succeeded_ids,
            failed=failed_ids,
        )

        if len(failed_ids) == len(app_ids):
            raise RuntimeError(
                f"앱 상세 수집 전체 실패: {len(failed_ids)}개 (appid={failed_ids})"
            )

    # ── 내부 메서드 ──────────────────────────────────────

    def _fetch_app_ids(self) -> list[int]:
        """tracked_games에서 is_active=TRUE인 app_id 목록 반환."""
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        base_sql = "SELECT appid FROM tracked_games WHERE is_active = TRUE ORDER BY last_discounted DESC"
        if self.app_id_limit is not None:
            rows = pg_hook.get_records(base_sql + " LIMIT %s", parameters=(self.app_id_limit,))
        else:
            rows = pg_hook.get_records(base_sql)

        return [row[0] for row in rows]

    @staticmethod
    def _extract_fields(appid: int, detail: dict) -> dict:
        """
        API 응답에서 dim_games·dim_genres에 필요한 필드만 추출하여 평탄화.

        genres는 Silver에서 dim_genres/dim_game_genres로 분리할 수 있도록
        list[dict] 형태 그대로 보존 (Polars가 list of struct으로 자동 처리).
        """
        platforms = detail.get("platforms", {})
        release = detail.get("release_date", {})
        recommendations = detail.get("recommendations", {})

        genres_raw = detail.get("genres", [])
        # genre id는 string으로 옴 → int 변환
        genres = [
            {"genre_id": int(g["id"]), "genre_name": g["description"]}
            for g in genres_raw
            if g.get("id", "").isdigit()
        ]

        return {
            "appid":                   appid,
            "name":                    detail.get("name", ""),
            "type":                    detail.get("type", ""),
            "is_free":                 detail.get("is_free", False),
            "required_age":            int(detail.get("required_age") or 0),
            "short_description":       detail.get("short_description", ""),
            "developers":              ", ".join(detail.get("developers") or []),
            "publishers":              ", ".join(detail.get("publishers") or []),
            "genres":                  genres,               # list[dict] → Polars list of struct
            "platforms_windows":       platforms.get("windows", False),
            "platforms_mac":           platforms.get("mac", False),
            "platforms_linux":         platforms.get("linux", False),
            "header_image":            detail.get("header_image", ""),
            "release_date_coming_soon": release.get("coming_soon", False),
            "release_date_str":        release.get("date", ""),   # "Jul 9, 2013" → Silver에서 파싱
            "recommendations_total":   recommendations.get("total"),
            "collected_at":            datetime.now(timezone.utc).isoformat(),
        }
