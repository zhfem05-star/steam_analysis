"""
Airflow 로그 정리 DAG

보관 기간이 지난 task 로그를 매일 삭제해 서버 디스크 증가를 막는다.

Airflow Variable (없으면 기본값):
  log_retention_days  로그 보관 일수 (기본 14)
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from callbacks.slack_callback import slack_fail_alert
from utils.log_cleanup import CleanupResult, cleanup_old_logs

log = logging.getLogger(__name__)

_DEFAULT_RETENTION_DAYS = 14


def _cleanup_logs() -> dict[str, int]:
    """
    설정된 로그 디렉터리에서 보관 기간이 지난 로그 삭제

    :return: 삭제 결과 (XCom 직렬화를 위해 NamedTuple 대신 dict)
    """
    log_dir = Path(conf.get("logging", "base_log_folder"))
    retention_days = int(Variable.get("log_retention_days", default_var=_DEFAULT_RETENTION_DAYS))

    result: CleanupResult = cleanup_old_logs(log_dir, retention_days)
    log.info(
        "로그 정리 완료: 경로=%s  보관=%d일  파일=%d개  크기=%.1fMB  빈 디렉터리=%d개",
        log_dir,
        retention_days,
        result.deleted_files,
        result.deleted_bytes / 1_048_576,
        result.deleted_dirs,
    )
    return result._asdict()


with DAG(
    dag_id="maintenance_01_log_cleanup",
    description="보관 기간이 지난 Airflow task 로그 삭제 (일별)",
    # 수집 DAG와 겹치지 않는 시각 (매시 정각 수집 직후)
    schedule="20 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:
    PythonOperator(
        task_id="cleanup_logs",
        python_callable=_cleanup_logs,
    )
