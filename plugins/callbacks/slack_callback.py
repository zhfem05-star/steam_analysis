"""
Slack 알림 콜백 함수

Airflow DAG/태스크의 on_failure_callback으로 등록해 사용.
SLACK_WEBHOOK_URL 환경변수에서 웹훅 URL을 읽는다.

사용 예시:
    from callbacks.slack_callback import slack_fail_alert

    with DAG(..., default_args={"on_failure_callback": slack_fail_alert}):
        ...
"""

from __future__ import annotations

import os

import requests

import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def slack_fail_alert(context) -> None:

    log = logging.getLogger(__name__)

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        log.warning("SLACK_WEBHOOK_URL 환경변수가 없어서 Slack 알림 건너뜀")
        return

    ti = context["task_instance"]
    exception = context.get("exception")

    message = {
        "attachments": [
            {
                "color": "#FF0000",
                "title": ":red_circle: Task 실패",
                "fields": [
                    {"title": "DAG", "value": ti.dag_id, "short": True},
                    {"title": "Task", "value": ti.task_id, "short": True},
                    {"title": "실행 시간", "value": str(context["execution_date"]), "short": True},
                    {"title": "에러", "value": str(exception) if exception else "알 수 없음", "short": False},
                    {"title": "로그", "value": ti.log_url, "short": False},
                ],
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=message, timeout=10)
        log.info("Slack 응답: %s / %s", response.status_code, response.text)
    except Exception as e:
        log.error("Slack 전송 실패: %s", e)

    requests.post(webhook_url, json=message, timeout=10)
