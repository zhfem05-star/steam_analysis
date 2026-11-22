"""
동접자 수집·관리 목록 공통 로직

- 동접자 Kafka 메시지 형식과 조회 상태 정의
- top 100 응답 변환, 개별 호출 대상 계산
- 스캔 결과에 따른 다음 스캔 간격 계산 (저조할수록 간격 증가)
- 월별 파티션 이름·범위 계산

I/O 없는 순수 함수만 두어 오프라인 테스트로 검증한다.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Literal, NamedTuple, TypedDict

# ── 상태·출처 값 ──────────────────────────────────────

PlayerStatus = Literal["ok", "not_found", "error"]
PlayerSource = Literal["top100", "individual", "scan"]

PLAYER_STATUS_OK: PlayerStatus = "ok"
PLAYER_STATUS_NOT_FOUND: PlayerStatus = "not_found"  # 404·result 42: 동접 데이터 없음
PLAYER_STATUS_ERROR: PlayerStatus = "error"  # 네트워크·5xx 등 수집 실패

SOURCE_TOP100: PlayerSource = "top100"
SOURCE_INDIVIDUAL: PlayerSource = "individual"
SOURCE_SCAN: PlayerSource = "scan"


class PlayerCountResult(NamedTuple):
    """
    개별 동접자 조회 결과

    :param player_count: 동접자 수. 데이터 없음·실패는 None
    :param status: 조회 상태
    """

    player_count: int | None
    status: PlayerStatus


class PlayerCountMessage(TypedDict):
    """
    steam.players·steam.player_scan 토픽 메시지

    - collected_at: 수집 슬롯 시각(Unix 초). 매시 수집은 정시, 스캔은 순회 시작 시각
    - fetched_at: 실제 API 호출 시각(Unix 초)
    - rank·peak_in_game은 top 100 응답에만 존재
    """

    appid: int
    snapshot_id: str
    collected_at: int
    fetched_at: int
    player_count: int | None
    status: PlayerStatus
    source: PlayerSource
    rank: int | None
    peak_in_game: int | None


# ── 메시지 변환 ──────────────────────────────────────


def top100_messages(
    ranks: list[dict[str, Any]],
    snapshot_id: str,
    collected_at: int,
    fetched_at: int,
) -> list[PlayerCountMessage]:
    """
    GetGamesByConcurrentPlayers 응답을 메시지로 변환

    :param ranks: 응답의 ranks 배열
    :param snapshot_id: 이번 수집 식별자
    :param collected_at: 수집 슬롯 Unix 초
    :param fetched_at: 호출 시각 Unix 초
    :raises ValueError: appid 또는 concurrent_in_game 누락
    """
    messages: list[PlayerCountMessage] = []
    for item in ranks:
        if "appid" not in item or "concurrent_in_game" not in item:
            raise ValueError(f"top 100 항목 필수 필드 누락: item 키={sorted(item)}")

        messages.append(
            {
                "appid": int(item["appid"]),
                "snapshot_id": snapshot_id,
                "collected_at": collected_at,
                "fetched_at": fetched_at,
                "player_count": int(item["concurrent_in_game"]),
                "status": PLAYER_STATUS_OK,
                "source": SOURCE_TOP100,
                "rank": item.get("rank"),
                "peak_in_game": item.get("peak_in_game"),
            }
        )
    return messages


def individual_message(
    appid: int,
    result: PlayerCountResult,
    snapshot_id: str,
    collected_at: int,
    fetched_at: int,
    source: PlayerSource,
) -> PlayerCountMessage:
    """개별 조회 결과를 메시지로 변환"""
    return {
        "appid": appid,
        "snapshot_id": snapshot_id,
        "collected_at": collected_at,
        "fetched_at": fetched_at,
        "player_count": result.player_count,
        "status": result.status,
        "source": source,
        "rank": None,
        "peak_in_game": None,
    }


def individual_targets(active_ids: Iterable[int], covered_ids: Iterable[int]) -> list[int]:
    """
    개별 호출 대상 계산

    - 이번 top 100 응답에 이미 있는 게임은 같은 시각 값이 있으므로 중복 호출하지 않음

    :param active_ids: 관리 목록 활성 게임
    :param covered_ids: 이번 top 100 응답 게임
    :return: 정렬된 개별 호출 대상
    """
    return sorted(set(active_ids) - set(covered_ids))


# ── 스캔 간격 ──────────────────────────────────────


class ScanSchedule(NamedTuple):
    """
    다음 스캔 일정

    :param interval_days: 갱신된 스캔 간격
    :param consecutive_low_scans: 갱신된 연속 저조 횟수
    :param delay_days: 이번 스캔 시각부터 다음 스캔까지 일수
    """

    interval_days: int
    consecutive_low_scans: int
    delay_days: int


def plan_next_scan(
    player_count: int | None,
    status: PlayerStatus,
    interval_days: int,
    consecutive_low_scans: int,
    low_threshold: int,
    max_interval_days: int,
) -> ScanSchedule:
    """
    스캔 결과로 다음 스캔 일정 계산

    - 저조(데이터 없음 또는 low_threshold 미만): 간격 2배, 최대 max_interval_days
    - 저조 아님: 간격 1일로 복귀
    - 수집 실패(error): 게임 상태를 알 수 없으므로 간격·횟수 유지하고 다음 날 재시도

    :param player_count: 스캔 동접자 수
    :param status: 조회 상태
    :param interval_days: 현재 스캔 간격
    :param consecutive_low_scans: 현재 연속 저조 횟수
    :param low_threshold: 저조 판단 기준 동접자 수
    :param max_interval_days: 최대 스캔 간격
    :return: 다음 스캔 일정
    """
    if status == PLAYER_STATUS_ERROR:
        return ScanSchedule(interval_days, consecutive_low_scans, delay_days=1)

    is_low = player_count is None or player_count < low_threshold
    if not is_low:
        return ScanSchedule(1, 0, delay_days=1)

    next_interval = min(max(interval_days, 1) * 2, max_interval_days)
    return ScanSchedule(next_interval, consecutive_low_scans + 1, delay_days=next_interval)


# ── 파티션 ──────────────────────────────────────


class MonthPartition(NamedTuple):
    """
    월별 파티션 정의

    - 경계는 세션 시간대와 무관하도록 UTC 오프셋을 붙인 TIMESTAMPTZ 문자열

    :param name: 파티션 테이블명
    :param start: 포함 시작
    :param end: 제외 끝
    """

    name: str
    start: str
    end: str


def month_partition(parent: str, collected_at: int) -> MonthPartition:
    """
    시각이 속한 UTC 월 파티션 계산

    :param parent: 부모 테이블명 (코드 상수만 사용)
    :param collected_at: Unix 초
    """
    moment = datetime.fromtimestamp(collected_at, tz=timezone.utc)
    start = moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
    return MonthPartition(
        name=f"{parent}_y{start.year:04d}m{start.month:02d}",
        start=f"{start.date().isoformat()} 00:00:00+00",
        end=f"{end.date().isoformat()} 00:00:00+00",
    )
