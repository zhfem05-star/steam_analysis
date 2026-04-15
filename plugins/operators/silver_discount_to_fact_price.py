"""
Bronze discount_games JSON → Silver fact_price_history 적재 오퍼레이터

S3 Bronze의 discount_games JSON 파일을 읽어
할인 가격 정보를 fact_price_history 테이블에 UPSERT한다.

Bronze JSON 구조:
  response.store_items[].appid
  response.store_items[].best_purchase_option.original_price_in_cents
  response.store_items[].best_purchase_option.final_price_in_cents
  response.store_items[].best_purchase_option.discount_pct
  response.store_items[].best_purchase_option.active_discounts[].discount_end_date

가격 단위: 원화 기준 원 단위 (KR cc 설정 시 센트가 아닌 원)
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from hooks.s3_hook import BUCKET_RAW, SteamS3Hook


class SilverDiscountToFactPriceOperator(BaseOperator):
    """
    Bronze discount_games JSON → fact_price_history UPSERT.

    :param s3_key_prefix:    읽을 Bronze S3 prefix (날짜 기준)
                             예: "discount_games/{{ ds_nodash }}"
    :param collected_at:     수집 날짜 (Jinja 템플릿 사용 가능). 기본값: execution_date 날짜
                             예: "{{ ds }}"
    :param postgres_conn_id: Airflow Postgres Connection ID
    :param s3_bucket:        소스 버킷 (기본값: steam-raw)
    :param aws_conn_id:      Airflow S3 Connection ID
    """

    template_fields = ("s3_key_prefix", "collected_at")

    def __init__(
        self,
        s3_key_prefix: str,
        collected_at: str = "{{ ds }}",
        postgres_conn_id: str = "analytics_db",
        s3_bucket: str = BUCKET_RAW,
        aws_conn_id: str = "minio_s3",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_key_prefix = s3_key_prefix
        self.collected_at = collected_at
        self.postgres_conn_id = postgres_conn_id
        self.s3_bucket = s3_bucket
        self.aws_conn_id = aws_conn_id

    def execute(self, context):  # noqa: ARG002
        s3_hook = SteamS3Hook(aws_conn_id=self.aws_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # 해당 날짜의 Bronze JSON 파일 탐색
        keys = s3_hook.list_keys(prefix=self.s3_key_prefix, bucket=self.s3_bucket)
        json_keys = [k for k in keys if k.endswith(".json")]

        if not json_keys:
            raise ValueError(f"처리할 JSON 파일 없음: prefix={self.s3_key_prefix}")

        # 날짜 내 파일이 여러 개면 가장 최근 것 사용
        target_key = sorted(json_keys)[-1]
        self.log.info("처리 대상 파일: %s", target_key)

        data = s3_hook.read_json(key=target_key, bucket=self.s3_bucket)
        store_items = data.get("response", {}).get("store_items", [])

        if not store_items:
            self.log.warning("store_items가 비어 있습니다: key=%s", target_key)
            return

        collected_date = date.fromisoformat(self.collected_at)
        rows = []

        for item in store_items:
            appid = item.get("appid")
            if appid is None:
                continue

            purchase = item.get("best_purchase_option") or {}
            is_free = item.get("is_free", False)

            # 가격은 KR cc 기준 원 단위 정수 문자열로 옴
            original_price = self._to_int(purchase.get("original_price_in_cents"))
            final_price = self._to_int(purchase.get("final_price_in_cents"))
            discount_pct = int(purchase.get("discount_pct") or 0)

            # 할인 종료일 (Unix timestamp → datetime)
            discount_end = None
            active_discounts = purchase.get("active_discounts") or []
            if active_discounts:
                end_ts = active_discounts[0].get("discount_end_date")
                if end_ts:
                    discount_end = datetime.fromtimestamp(int(end_ts), tz=timezone.utc)

            rows.append({
                "app_id":          appid,
                "currency":        "KRW",
                "initial_price":   original_price,
                "final_price":     final_price,
                "discount_percent": discount_pct,
                "is_free":         is_free,
                "collected_at":    collected_date,
                "discount_end_at": discount_end,
            })

        self.log.info("UPSERT 대상: %d건", len(rows))
        self._upsert_fact_price(pg_hook, rows)

    # ── 내부 메서드 ──────────────────────────────────────

    @staticmethod
    def _to_int(value) -> int | None:
        """문자열 또는 숫자를 int로 변환. 실패 시 None."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _upsert_fact_price(self, pg_hook: PostgresHook, rows: list[dict]) -> None:
        sql = """
            INSERT INTO fact_price_history (
                app_id, currency, initial_price, final_price,
                discount_percent, is_free, discount_end_at, collected_at
            ) VALUES (
                %(app_id)s, %(currency)s, %(initial_price)s, %(final_price)s,
                %(discount_percent)s, %(is_free)s, %(discount_end_at)s, %(collected_at)s
            )
            ON CONFLICT (app_id, collected_at) DO UPDATE SET
                currency         = EXCLUDED.currency,
                initial_price    = EXCLUDED.initial_price,
                final_price      = EXCLUDED.final_price,
                discount_percent = EXCLUDED.discount_percent,
                is_free          = EXCLUDED.is_free,
                discount_end_at  = EXCLUDED.discount_end_at
        """
        conn = pg_hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        self.log.info("fact_price_history UPSERT 완료: %d건", len(rows))
