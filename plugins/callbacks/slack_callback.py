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

def _send_slack(message: dict) -> None:
    """Slack 웹훅으로 메시지 전송."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        log.warning("SLACK_WEBHOOK_URL 환경변수가 없어서 Slack 알림 건너뜀")
        return
    try:
        response = requests.post(webhook_url, json=message, timeout=10)
        log.info("Slack 응답: %s / %s", response.status_code, response.text)
    except Exception as e:
        log.error("Slack 전송 실패: %s", e)


def slack_fail_alert(context) -> None:
    """Task 실패 시 Airflow on_failure_callback으로 호출되는 알림."""
    ti = context["task_instance"]
    exception = context.get("exception")

    message = {
        "attachments": [
            {
                "color": "#FF0000",
                "title": ":red_circle: Task 실패",
                "fields": [
                    {"title": "DAG",    "value": ti.dag_id,  "short": True},
                    {"title": "Task",   "value": ti.task_id, "short": True},
                    {"title": "실행 시간", "value": str(context["execution_date"]), "short": True},
                    {"title": "에러",   "value": str(exception) if exception else "알 수 없음", "short": False},
                    {"title": "로그",   "value": ti.log_url, "short": False},
                ],
            }
        ]
    }
    _send_slack(message)


def slack_collect_summary(
    dag_id: str,
    task_id: str,
    log_url: str,
    succeeded: list,
    failed: list,
) -> None:
    """
    수집 완료 후 결과 요약을 Slack으로 전송.

    - 전체 성공: 초록 알림
    - 일부 실패: 노란 경고 + 실패 appid 목록
    - 전체 실패: 빨간 알림 + 실패 appid 목록
    """
    total = len(succeeded) + len(failed)

    if not failed:
        color, title = "#36A64F", ":white_check_mark: 수집 완료"
    elif len(failed) < total:
        color, title = "#FFA500", ":warning: 수집 완료 (일부 실패)"
    else:
        color, title = "#FF0000", ":red_circle: 수집 전체 실패"

    fields = [
        {"title": "DAG",    "value": dag_id,  "short": True},
        {"title": "Task",   "value": task_id, "short": True},
        {"title": "성공",   "value": f"{len(succeeded)}개", "short": True},
        {"title": "실패",   "value": f"{len(failed)}개",    "short": True},
    ]
    if failed:
        fields.append({
            "title": "실패 appid",
            "value": ", ".join(str(a) for a in failed),
            "short": False,
        })
    fields.append({"title": "로그", "value": log_url, "short": False})

    _send_slack({"attachments": [{"color": color, "title": title, "fields": fields}]})
