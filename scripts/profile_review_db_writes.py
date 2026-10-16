"""고정 리뷰 집계 입력으로 DB 호출 방식 비교

세션 전용 TEMP 테이블을 사용하며 운영 테이블에는 접근하지 않음
입력 생성·검증을 제외한 INSERT 및 commit 구간을 측정
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from datetime import date, datetime, timezone
from importlib.metadata import version
from pathlib import Path
from time import perf_counter
from typing import Literal

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import cursor as PgCursor
from psycopg2.extras import execute_values

Method = Literal["execute_loop", "executemany", "execute_values"]
ReviewRow = tuple[int, str, date, int]
METHODS: tuple[Method, ...] = ("execute_loop", "executemany", "execute_values")
SQL = """
    INSERT INTO profile_review_daily (app_id, language, review_date, review_count)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (app_id, language, review_date)
    DO UPDATE SET review_count = EXCLUDED.review_count, updated_at = NOW()
"""


def write_rows(cur: PgCursor, rows: list[ReviewRow], method: Method) -> None:
    """동일 SQL 의미를 유지하며 호출 방식만 변경"""
    if method == "execute_loop":
        for row in rows:
            cur.execute(SQL, row)
    elif method == "executemany":
        cur.executemany(SQL, rows)
    else:
        execute_values(cur, SQL.replace("(%s, %s, %s, %s)", "%s"), rows, page_size=1000)


def main() -> None:
    """입력과 실측 표본·환경·측정 범위를 JSON으로 기록"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rows < 1 or args.repeats < 3:
        raise ValueError("입력은 1행 이상, 반복은 3회 이상이어야 합니다")

    rows: list[ReviewRow] = [
        (100000 + i, "english", date(2026, 10, 15), i % 100 + 1)
        for i in range(args.rows)
    ]
    samples: dict[Method, list[float]] = {method: [] for method in METHODS}
    conn = PostgresHook(postgres_conn_id="analytics_db").get_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            postgres_version = cur.fetchone()[0]
            cur.execute("""
                CREATE TEMP TABLE profile_review_daily (
                    app_id INTEGER NOT NULL,
                    language TEXT NOT NULL,
                    review_date DATE NOT NULL,
                    review_count INTEGER NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (app_id, language, review_date)
                ) ON COMMIT PRESERVE ROWS
            """)
            conn.commit()

            # 방식별로 소량 예열 후 매번 빈 테이블에서 동일 입력을 측정
            for method in METHODS:
                cur.execute("TRUNCATE pg_temp.profile_review_daily")
                write_rows(cur, rows[:100], method)
                conn.commit()

            for repeat in range(args.repeats):
                # 後続方式だけが常に有利にならないよう実行順をローテーション
                offset = repeat % len(METHODS)
                for method in METHODS[offset:] + METHODS[:offset]:
                    cur.execute("TRUNCATE pg_temp.profile_review_daily")
                    conn.commit()
                    started = perf_counter()
                    write_rows(cur, rows, method)
                    conn.commit()
                    elapsed = perf_counter() - started
                    cur.execute("""
                        SELECT app_id, language, review_date, review_count
                        FROM pg_temp.profile_review_daily ORDER BY app_id
                    """)
                    if cur.fetchall() != rows:
                        raise RuntimeError(f"DB 적재 결과가 입력과 다릅니다: method={method}")
                    conn.commit()
                    samples[method].append(elapsed)
                    print(f"{method} repeat={repeat + 1} seconds={elapsed:.6f}", flush=True)
    finally:
        conn.close()

    results = {
        method: {
            "seconds": times,
            "median_seconds": statistics.median(times),
            "min_seconds": min(times),
            "max_seconds": max(times),
            "rows_per_second_at_median": args.rows / statistics.median(times),
        }
        for method, times in samples.items()
    }
    report = {
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version,
        "postgres": postgres_version,
        "packages": {name: version(name) for name in ("psycopg2-binary", "apache-airflow")},
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "input_sha256": hashlib.sha256(json.dumps(rows, default=str).encode()).hexdigest(),
        "rows": args.rows,
        "repeats": args.repeats,
        "scope": "fresh TEMP table inserts and one commit; connection reused; no durable table WAL; excludes input generation, truncate and result validation",
        "batch_page_size": 1000,
        "results_verified": True,
        "results": results,
    }
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(results, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
