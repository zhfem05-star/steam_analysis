import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_AIRFLOW_HOME = Path("/tmp/steam_project_airflow_home")
_AIRFLOW_HOME.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("AIRFLOW_HOME", _AIRFLOW_HOME.as_posix())
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    f"sqlite:///{_AIRFLOW_HOME.as_posix()}/airflow.db",
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

# airflow를 import하는 아래 두 모듈은 반드시 위 환경변수 설정 이후에 와야 함
from hooks.s3_hook import SteamS3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@pytest.fixture
def mock_s3_hook():
    return MagicMock(spec=SteamS3Hook)


@pytest.fixture
def mock_pg_hook():
    hook = MagicMock(spec=PostgresHook)
    # get_conn().cursor()를 with문으로 쓰는 오퍼레이터(silver_*)를 위한 배선
    cursor = MagicMock()
    hook.get_conn.return_value.cursor.return_value.__enter__.return_value = cursor
    hook.mock_cursor = cursor  # 테스트에서 바로 참조하기 위한 shortcut
    return hook


@pytest.fixture
def dummy_context():
    ti = MagicMock()
    ti.dag_id, ti.task_id, ti.log_url = "test_dag", "test_task", "http://test"
    return {"ti": ti}