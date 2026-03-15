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


def slack_fail_alert(context) -> None:
    """
    태스크 실패 시 Slack에 알림을 전송한다.

    전송 정보:
      - DAG ID / Task ID
      - 실행 시간 (execution_date)
      - 에러 메시지
      - Airflow 로그 URL
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
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

    requests.post(webhook_url, json=message, timeout=10)
