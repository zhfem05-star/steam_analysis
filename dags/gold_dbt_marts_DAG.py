"""
dbt Gold 마트 빌드 DAG

DockerOperator로 steam-dbt 컨테이너를 실행하여 dbt 모델을 빌드한다.
dbt 컨테이너는 실행 후 자동 종료 → Airflow와 메모리 완전 격리.

빌드 대상 모델:
  - mart_game_overview      : 게임 종합 정보 (dim + 최신 가격 + 장르)
  - mart_player_daily_avg   : 동접자 일평균
  - mart_discount_analysis  : 할인율 이력 분석
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from callbacks.slack_callback import slack_fail_alert

_DBT_CMD_BASE = "dbt run --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt"
_NETWORK = "steam-project_default"   # docker-compose 기본 네트워크명

with DAG(
    dag_id="steam_gold_02_dbt_marts",
    description="dbt DockerOperator → Gold 마트 테이블 빌드 (격리 실행)",
    schedule="0 12 * * *",     # 매일 12:00 UTC (Silver + Gold 01 완료 후)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "gold", "dbt"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    # staging 뷰 빌드
    dbt_staging = DockerOperator(
        task_id="dbt_staging",
        image="steam-dbt:latest",
        command=f"{_DBT_CMD_BASE} --select staging",
        network_mode=_NETWORK,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # marts 테이블 빌드
    dbt_marts = DockerOperator(
        task_id="dbt_marts",
        image="steam-dbt:latest",
        command=f"{_DBT_CMD_BASE} --select marts",
        network_mode=_NETWORK,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    dbt_staging >> dbt_marts
