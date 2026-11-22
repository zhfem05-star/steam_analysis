"""
스토어 전체 스냅샷(S3) → PostgreSQL 카탈로그·가격 구간 적재 오퍼레이터

- producer manifest와 consumer parquet을 읽어 완료된 스냅샷인지 검증
- 게임 목록은 dim_apps upsert, 가격·리뷰 요약은 변경 구간만 기록
- 스냅샷 하나의 적용은 단일 트랜잭션에서 수행하고 silver_snapshot_log에 기록

적용 거부 조건:
  - manifest 없음 (producer 미완료)
  - 실제 고유 appid 수와 manifest 건수 불일치
  - 이미 적용한 스냅샷보다 오래된 스냅샷
  - 직전 상태 대비 건수가 min_coverage 비율 미만으로 급감 (API 이상 응답 의심)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from psycopg2.extensions import cursor as Cursor

import polars as pl
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hooks.s3_hook import BUCKET_RAW, SteamS3Hook
from utils.store_catalog import (
    MANIFEST_FILENAME,
    IntervalDiff,
    SnapshotManifest,
    diff_snapshot,
)

# 적용 결과 요약 (XCom 반환값)
ApplySummary = dict[str, Any]


class IntervalTable(NamedTuple):
    """
    변경 구간 테이블 정의

    :param table: 테이블명 (코드 상수만 사용, 외부 입력 금지)
    :param value_columns: 변경 판단·저장 컬럼
    :param timestamp_columns: Unix 초 → TIMESTAMPTZ 변환이 필요한 컬럼
    """

    table: str
    value_columns: list[str]
    timestamp_columns: frozenset[str]


PRICE_INTERVALS = IntervalTable(
    table="fact_price_intervals",
    value_columns=[
        "is_free",
        "has_price",
        "original_price_cents",
        "final_price_cents",
        "discount_pct",
        "discount_end_at",
    ],
    timestamp_columns=frozenset({"discount_end_at"}),
)

REVIEW_SUMMARY_INTERVALS = IntervalTable(
    table="fact_review_summary_intervals",
    value_columns=["review_count", "percent_positive", "review_score"],
    timestamp_columns=frozenset(),
)


class SilverStoreSnapshotOperator(BaseOperator):
    """
    스냅샷 검증·적용 공통 흐름

    - 하위 클래스는 source와 apply_snapshot만 정의

    :param snapshot_prefix: producer·consumer와 같은 S3 prefix (Jinja 템플릿)
    :param min_coverage: 직전 상태 대비 허용 최소 건수 비율
    :param aws_conn_id: Airflow에 등록한 S3 연결 ID
    :param postgres_conn_id: Airflow에 등록한 PostgreSQL 연결 ID
    :param s3_bucket: 스냅샷 버킷
    """

    source = ""
    template_fields = ("snapshot_prefix",)

    def __init__(
        self,
        snapshot_prefix: str,
        min_coverage: float = 0.9,
        aws_conn_id: str = "s3_storage",
        postgres_conn_id: str = "analytics_db",
        s3_bucket: str = BUCKET_RAW,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not 0 < min_coverage <= 1:
            raise ValueError(
                f"min_coverage는 0 초과 1 이하여야 합니다: min_coverage={min_coverage!r}"
            )

        self.snapshot_prefix = snapshot_prefix
        self.min_coverage = min_coverage
        self.aws_conn_id = aws_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.s3_bucket = s3_bucket

    def apply_snapshot(
        self,
        cursor: Cursor,
        manifest: SnapshotManifest,
        snapshot: pl.DataFrame,
    ) -> ApplySummary:
        """검증을 통과한 스냅샷을 테이블에 반영 (하위 클래스 구현)"""
        raise NotImplementedError

    # ── 실행 ──────────────────────────────────────

    def execute(self, context: Context) -> ApplySummary:  # noqa: ARG002
        manifest, snapshot = self.load_snapshot(
            SteamS3Hook(aws_conn_id=self.aws_conn_id)
        )

        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id).get_conn()
        try:
            with conn, conn.cursor() as cursor:
                if self._already_applied(cursor, manifest):
                    self.log.info(
                        "이미 적용한 스냅샷, 건너뜀: source=%s  snapshot=%s",
                        self.source,
                        manifest["snapshot_id"],
                    )
                    return {
                        "source": self.source,
                        "snapshot_id": manifest["snapshot_id"],
                        "skipped": True,
                    }

                summary = self.apply_snapshot(cursor, manifest, snapshot)
                self._record_applied(cursor, manifest)
        finally:
            conn.close()

        self.log.info("스냅샷 적용 완료: %s", summary)
        return summary

    # ── 스냅샷 로드·검증 ──────────────────────────────────────

    def load_snapshot(
        self, s3_hook: SteamS3Hook
    ) -> tuple[SnapshotManifest, pl.DataFrame]:
        """
        manifest와 parquet을 읽어 완료된 스냅샷인지 검증

        - consumer 재처리로 생긴 중복 메시지는 appid 기준으로 제거
        - 다른 스냅샷의 지연 메시지가 같은 prefix에 섞여도 snapshot_id로 걸러냄

        :return: (manifest, appid 고유 스냅샷)
        :raises FileNotFoundError: manifest 또는 parquet 없음
        :raises ValueError: 고유 appid 수와 manifest 건수 불일치
        """
        manifest_key = f"{self.snapshot_prefix}/{MANIFEST_FILENAME}"
        if not s3_hook.key_exists(key=manifest_key, bucket=self.s3_bucket):
            raise FileNotFoundError(
                f"스냅샷 manifest 없음 (producer 미완료): s3://{self.s3_bucket}/{manifest_key}"
            )
        manifest: SnapshotManifest = s3_hook.read_json(
            key=manifest_key, bucket=self.s3_bucket
        )  # type: ignore[assignment]

        keys = sorted(
            key
            for key in s3_hook.list_keys(
                prefix=f"{self.snapshot_prefix}/", bucket=self.s3_bucket
            )
            if key.endswith(".parquet")
        )
        if not keys:
            raise FileNotFoundError(
                f"스냅샷 parquet 없음 (consumer 미완료): s3://{self.s3_bucket}/{self.snapshot_prefix}/"
            )

        frames = [s3_hook.read_parquet(key=key, bucket=self.s3_bucket) for key in keys]
        snapshot = (
            pl.concat(frames, how="diagonal_relaxed")
            .filter(pl.col("snapshot_id") == manifest["snapshot_id"])
            .unique(subset=["appid"], keep="last", maintain_order=True)
            .rename({"appid": "app_id"})
        )

        if snapshot.height != manifest["records"]:
            raise ValueError(
                "스냅샷 건수 불일치: "
                f"source={manifest['source']}, snapshot={manifest['snapshot_id']}, "
                f"manifest={manifest['records']}, 적재된 고유 appid={snapshot.height}, 파일={len(keys)}개"
            )
        return manifest, snapshot

    def _require_coverage(self, label: str, previous: int, current: int) -> None:
        """
        직전 상태 대비 건수 급감 차단

        :raises ValueError: current가 previous × min_coverage 미만
        """
        if previous and current < previous * self.min_coverage:
            raise ValueError(
                f"스냅샷 건수 급감으로 적용 중단: 대상={label}, 직전={previous}, "
                f"이번={current}, 허용 최소 비율={self.min_coverage}"
            )

    # ── 스냅샷 기록 ──────────────────────────────────────

    def _already_applied(self, cursor: Cursor, manifest: SnapshotManifest) -> bool:
        """
        같은 스냅샷 적용 여부 확인 및 순서 역전 차단

        :raises ValueError: 이미 적용한 스냅샷보다 수집 시각이 이르거나 같음
        """
        cursor.execute(
            "SELECT 1 FROM silver_snapshot_log WHERE source = %s AND snapshot_id = %s",
            (self.source, manifest["snapshot_id"]),
        )
        if cursor.fetchone():
            return True

        cursor.execute(
            "SELECT snapshot_id, EXTRACT(EPOCH FROM collected_at)::BIGINT "
            "FROM silver_snapshot_log WHERE source = %s ORDER BY collected_at DESC LIMIT 1",
            (self.source,),
        )
        latest = cursor.fetchone()
        if latest and latest[1] >= manifest["collected_at"]:
            raise ValueError(
                f"이미 적용한 스냅샷보다 오래된 스냅샷: source={self.source}, "
                f"이번={manifest['snapshot_id']}({manifest['collected_at']}), "
                f"최신 적용={latest[0]}({latest[1]})"
            )
        return False

    def _record_applied(self, cursor: Cursor, manifest: SnapshotManifest) -> None:
        cursor.execute(
            "INSERT INTO silver_snapshot_log (source, snapshot_id, collected_at, records) "
            "VALUES (%s, %s, to_timestamp(%s), %s)",
            (
                self.source,
                manifest["snapshot_id"],
                manifest["collected_at"],
                manifest["records"],
            ),
        )

    # ── DB 공통 처리 ──────────────────────────────────────

    @staticmethod
    def _load_keys(cursor: Cursor, keys: list[int]) -> None:
        """대상 키를 트랜잭션 임시 테이블 tmp_snapshot_keys에 적재"""
        # psycopg2는 Airflow 이미지에만 있으므로 오프라인 테스트 import를 위해 지연 import
        from psycopg2.extras import execute_values

        cursor.execute(
            "CREATE TEMP TABLE IF NOT EXISTS tmp_snapshot_keys (app_id INTEGER PRIMARY KEY) ON COMMIT DROP"
        )
        cursor.execute("TRUNCATE tmp_snapshot_keys")
        if keys:
            execute_values(
                cursor,
                "INSERT INTO tmp_snapshot_keys (app_id) VALUES %s",
                [(key,) for key in keys],
                page_size=1000,
            )

    def apply_intervals(
        self,
        cursor: Cursor,
        spec: IntervalTable,
        snapshot: pl.DataFrame,
        observed_at: int,
    ) -> dict[str, int]:
        """
        스냅샷을 변경 구간 테이블에 반영

        - 바뀐 구간을 먼저 닫은 뒤 새 구간을 넣어 열린 구간 유일성 유지

        :param spec: 대상 구간 테이블 정의
        :param snapshot: app_id + value_columns
        :param observed_at: 스냅샷 수집 Unix 초
        :return: touch·close·insert 건수
        """
        from psycopg2.extras import execute_values

        open_intervals = self._fetch_open_intervals(cursor, spec)
        self._require_coverage(spec.table, open_intervals.height, snapshot.height)

        diff: IntervalDiff = diff_snapshot(
            open_intervals,
            snapshot.select(["app_id", *spec.value_columns]),
            key="app_id",
            value_columns=spec.value_columns,
        )

        self._load_keys(cursor, diff.close_keys)
        cursor.execute(
            f"UPDATE {spec.table} AS t SET valid_to = to_timestamp(%s) "  # noqa: S608 (상수 테이블명)
            "FROM tmp_snapshot_keys AS k WHERE t.app_id = k.app_id AND t.valid_to IS NULL",
            (observed_at,),
        )

        self._load_keys(cursor, diff.touch_keys)
        cursor.execute(
            f"UPDATE {spec.table} AS t SET last_seen_at = to_timestamp(%s) "  # noqa: S608 (상수 테이블명)
            "FROM tmp_snapshot_keys AS k WHERE t.app_id = k.app_id AND t.valid_to IS NULL",
            (observed_at,),
        )

        value_sql = ", ".join(spec.value_columns)
        value_template = ", ".join(
            "to_timestamp(%s)" if column in spec.timestamp_columns else "%s"
            for column in spec.value_columns
        )
        rows = [
            (row[0], observed_at, observed_at, *row[1:])
            for row in diff.insert_rows.select(
                ["app_id", *spec.value_columns]
            ).iter_rows()
        ]
        if rows:
            execute_values(
                cursor,
                f"INSERT INTO {spec.table} (app_id, valid_from, last_seen_at, {value_sql}) VALUES %s",  # noqa: S608
                rows,
                template=f"(%s, to_timestamp(%s), to_timestamp(%s), {value_template})",
                page_size=1000,
            )

        return {
            "touched": len(diff.touch_keys),
            "closed": len(diff.close_keys),
            "inserted": len(rows),
        }

    @staticmethod
    def _fetch_open_intervals(cursor: Cursor, spec: IntervalTable) -> pl.DataFrame:
        """열린 구간을 스냅샷과 같은 표현(시각은 Unix 초)으로 조회"""
        select_columns = ", ".join(
            f"EXTRACT(EPOCH FROM {column})::BIGINT"
            if column in spec.timestamp_columns
            else column
            for column in spec.value_columns
        )
        cursor.execute(
            f"SELECT app_id, {select_columns} FROM {spec.table} WHERE valid_to IS NULL"  # noqa: S608
        )
        return pl.DataFrame(
            cursor.fetchall(),
            schema=["app_id", *spec.value_columns],
            orient="row",
            infer_schema_length=None,
        )


class SilverAppCatalogOperator(SilverStoreSnapshotOperator):
    """게임 목록 스냅샷을 dim_apps에 반영 (신규·변경 upsert, 사라진 게임 is_listed=FALSE)"""

    source = "app_list"

    def apply_snapshot(
        self,
        cursor: Cursor,
        manifest: SnapshotManifest,
        snapshot: pl.DataFrame,
    ) -> ApplySummary:
        from psycopg2.extras import execute_values

        observed_at = manifest["collected_at"]

        cursor.execute("SELECT count(*) FROM dim_apps WHERE is_listed")
        listed_before = cursor.fetchone()[0]
        self._require_coverage("dim_apps", listed_before, snapshot.height)

        rows = list(
            snapshot.select(
                ["app_id", "name", "last_modified", "price_change_number"]
            ).iter_rows()
        )
        execute_values(
            cursor,
            "INSERT INTO dim_apps (app_id, name, last_modified, price_change_number, "
            "is_listed, first_seen_at, last_seen_at, delisted_at) VALUES %s "
            "ON CONFLICT (app_id) DO UPDATE SET "
            "name = EXCLUDED.name, last_modified = EXCLUDED.last_modified, "
            "price_change_number = EXCLUDED.price_change_number, "
            "is_listed = TRUE, last_seen_at = EXCLUDED.last_seen_at, delisted_at = NULL",
            [
                (app_id, name, modified, change, observed_at, observed_at)
                for app_id, name, modified, change in rows
            ],
            template="(%s, %s, to_timestamp(%s), %s, TRUE, to_timestamp(%s), to_timestamp(%s), NULL)",
            page_size=1000,
        )

        self._load_keys(cursor, snapshot["app_id"].to_list())
        cursor.execute(
            "UPDATE dim_apps AS d SET is_listed = FALSE, delisted_at = to_timestamp(%s) "
            "WHERE d.is_listed AND NOT EXISTS (SELECT 1 FROM tmp_snapshot_keys AS k WHERE k.app_id = d.app_id)",
            (observed_at,),
        )
        delisted = cursor.rowcount

        return {
            "source": self.source,
            "snapshot_id": manifest["snapshot_id"],
            "upserted": len(rows),
            "delisted": delisted,
        }


class SilverStorePriceIntervalsOperator(SilverStoreSnapshotOperator):
    """가격·리뷰 요약 스냅샷을 각각의 변경 구간 테이블에 반영"""

    source = "store_prices"

    def apply_snapshot(
        self,
        cursor: Cursor,
        manifest: SnapshotManifest,
        snapshot: pl.DataFrame,
    ) -> ApplySummary:
        observed_at = manifest["collected_at"]
        return {
            "source": self.source,
            "snapshot_id": manifest["snapshot_id"],
            "price": self.apply_intervals(
                cursor, PRICE_INTERVALS, snapshot, observed_at
            ),
            "review_summary": self.apply_intervals(
                cursor, REVIEW_SUMMARY_INTERVALS, snapshot, observed_at
            ),
        }
