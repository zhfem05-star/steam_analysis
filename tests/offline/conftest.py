"""외부 서비스 없이 실제 프로젝트 코드와 Polars를 검증한다. Airflow 연결부만 대체한다.
실행: python -m pytest --confcutdir=tests/offline tests/offline
"""
import importlib.util
import logging
import sys
import types
from unittest.mock import MagicMock
import pytest

try:
    from airflow.models import BaseOperator
except ImportError:
    def module(name, **members):
        result = types.ModuleType(name)
        result.__path__ = []
        result.__dict__.update(members)
        sys.modules[name] = result
        return result
    class BaseOperator:
        def __init__(self, task_id, **kwargs):
            self.task_id = task_id
            self.log = logging.getLogger(task_id)
    class UnavailableHook:
        def __init__(self, *args, **kwargs):
            raise AssertionError("Service hook must be mocked in offline tests")
    module("airflow")
    module("airflow.models", BaseOperator=BaseOperator, Variable=MagicMock())
    module("airflow.hooks")
    module("airflow.hooks.base", BaseHook=UnavailableHook)
    for name in ("airflow.providers", "airflow.providers.postgres",
                 "airflow.providers.postgres.hooks", "airflow.providers.amazon",
                 "airflow.providers.amazon.aws", "airflow.providers.amazon.aws.hooks"):
        module(name)
    module("airflow.providers.postgres.hooks.postgres", PostgresHook=UnavailableHook)
    module("airflow.providers.amazon.aws.hooks.s3", S3Hook=UnavailableHook)

@pytest.fixture(autouse=True)
def no_external_io(monkeypatch):
    import requests
    def blocked(*args, **kwargs):
        raise AssertionError("Offline tests must not make network requests")
    monkeypatch.setattr(requests.sessions.Session, "request", blocked)
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "")
