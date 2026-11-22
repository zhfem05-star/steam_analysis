"""
재구성 4단계(동접자 top 100·관리 목록·스캔) 오프라인 검증

- 스캔 간격 규칙, 개별 호출 대상 계산, 월 파티션 경계 (순수 함수)
- token bucket 요청 간격과 429 대응
- 개별 동접자 조회의 데이터 없음(404)·429 구분
- 매시 producer의 top 100 중복 호출 제외, 슬롯 지연 차단, 실패 한도
- 대상 0건 스냅샷의 Silver 로드
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

import hooks.steam_api as steam_api_module
from hooks.steam_api import SteamApiHook
from operators.silver_players_to_db import SilverPlayerScanOperator
from operators.steam_players_to_kafka import (
    SteamPlayerCountsToKafkaOperator,
    SteamPlayerScanToKafkaOperator,
)
from utils.player_watchlist import (
    PLAYER_STATUS_ERROR,
    PLAYER_STATUS_NOT_FOUND,
    PLAYER_STATUS_OK,
    PlayerCountResult,
    individual_targets,
    month_partition,
    plan_next_scan,
    top100_messages,
)
from utils.rate_limit import RateLimitedCaller, SteamRateLimitError, TokenBucket

SLOT_AT = 1795327200  # 2026-11-22T03:00:00Z


# ── 스캔 간격 ──────────────────────────────────────


def schedule(count: int | None, status: str, interval: int = 1, low: int = 0):
    return plan_next_scan(count, status, interval, low, low_threshold=10, max_interval_days=30)  # type: ignore[arg-type]


def test_active_game_resets_interval_to_one_day() -> None:
    result = schedule(500, PLAYER_STATUS_OK, interval=8, low=3)

    assert tuple(result) == (1, 0, 1)


def test_low_or_missing_game_doubles_interval_up_to_max() -> None:
    assert tuple(schedule(3, PLAYER_STATUS_OK, interval=4, low=2)) == (8, 3, 8)
    assert tuple(schedule(None, PLAYER_STATUS_NOT_FOUND, interval=1)) == (2, 1, 2)
    assert tuple(schedule(0, PLAYER_STATUS_OK, interval=16, low=4)) == (30, 5, 30), "최대 30일"


def test_failed_scan_keeps_state_and_retries_next_day() -> None:
    result = schedule(None, PLAYER_STATUS_ERROR, interval=16, low=4)

    assert tuple(result) == (16, 4, 1), "실패는 저조로 세지 않음"


# ── 대상 계산·메시지 ──────────────────────────────────────


def test_individual_targets_exclude_games_already_in_top100() -> None:
    assert individual_targets([30, 10, 20, 10], [20, 40]) == [10, 30]


def test_top100_messages_keep_rank_and_require_count() -> None:
    ranks = [{"rank": 1, "appid": 730, "concurrent_in_game": 600000, "peak_in_game": 1200000}]

    message = top100_messages(ranks, "s", SLOT_AT, SLOT_AT + 5)[0]

    assert (message["appid"], message["player_count"], message["rank"]) == (730, 600000, 1)
    assert (message["collected_at"], message["fetched_at"]) == (SLOT_AT, SLOT_AT + 5)
    with pytest.raises(ValueError, match="필수 필드 누락"):
        top100_messages([{"appid": 1}], "s", SLOT_AT, SLOT_AT)


def test_month_partition_uses_utc_bounds_and_rolls_year() -> None:
    november = month_partition("fact_player_counts", SLOT_AT)
    december = month_partition("fact_player_counts", 1798761599)  # 2026-12-31T23:59:59Z

    assert november == (
        "fact_player_counts_y2026m11",
        "2026-11-01 00:00:00+00",
        "2026-12-01 00:00:00+00",
    )
    assert december.name == "fact_player_counts_y2026m12"
    assert december.end == "2027-01-01 00:00:00+00"


# ── 요청률 제어 ──────────────────────────────────────


class FakeClock:
    """sleep만큼 시간이 흐르는 테스트 시계"""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(round(seconds, 6))
        self.now += seconds


def test_token_bucket_spaces_requests_by_rate() -> None:
    clock = FakeClock()
    bucket = TokenBucket(5.0, clock=clock, sleep=clock.sleep)

    for _ in range(3):
        bucket.acquire()

    assert clock.sleeps == [0.2, 0.2], "첫 요청은 즉시, 이후 0.2초 간격"


def test_throttled_call_waits_retry_after_and_halves_rate() -> None:
    clock = FakeClock()
    bucket = TokenBucket(4.0, clock=clock, sleep=clock.sleep)
    caller = RateLimitedCaller(bucket)
    responses: list[Any] = [SteamRateLimitError("429", retry_after=30), 7]

    def call() -> int:
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    assert caller.call(call) == 7
    assert 30 in clock.sleeps, clock.sleeps
    assert bucket.rate_per_sec == 2.0
    assert (caller.calls, caller.throttled) == (2, 1)


def test_repeated_throttling_stops_run() -> None:
    clock = FakeClock()
    caller = RateLimitedCaller(TokenBucket(5.0, clock=clock, sleep=clock.sleep), max_consecutive_throttles=2)

    def always_throttled() -> int:
        raise SteamRateLimitError("429")

    with pytest.raises(RuntimeError, match="연속 429 3회"):
        caller.call(always_throttled)


# ── 개별 동접자 조회 ──────────────────────────────────────


def fake_response(status_code: int, body: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> MagicMock:
    return MagicMock(
        ok=200 <= status_code < 300,
        status_code=status_code,
        text=str(body),
        headers=headers or {},
        json=MagicMock(return_value=body or {}),
    )


def test_fetch_current_players_separates_ok_not_found_and_throttled(monkeypatch: pytest.MonkeyPatch) -> None:
    hook = SteamApiHook(api_key="k")
    responses = {
        1: fake_response(200, {"response": {"player_count": 42, "result": 1}}),
        2: fake_response(404, {"response": {"result": 42}}),
        3: fake_response(429, headers={"Retry-After": "15"}),
        4: fake_response(503),
    }
    monkeypatch.setattr(
        steam_api_module.requests,
        "get",
        lambda url, params, timeout: responses[params["appid"]],  # noqa: ARG005
    )

    assert hook.fetch_current_players(1) == (42, PLAYER_STATUS_OK)
    assert hook.fetch_current_players(2) == (None, PLAYER_STATUS_NOT_FOUND)
    with pytest.raises(SteamRateLimitError) as throttled:
        hook.fetch_current_players(3)
    assert throttled.value.retry_after == 15
    with pytest.raises(requests.HTTPError):
        hook.fetch_current_players(4)


# ── producer ──────────────────────────────────────


class FakeKafkaHook:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def send(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:  # noqa: ARG002
        self.sent.append(value)

    def flush(self) -> None:
        pass


def player_api(results: dict[int, PlayerCountResult | Exception]) -> MagicMock:
    api = MagicMock()
    api.get_top_concurrent_players.return_value = [
        {"rank": 1, "appid": 730, "concurrent_in_game": 600000, "peak_in_game": 1200000},
        {"rank": 2, "appid": 570, "concurrent_in_game": 500000, "peak_in_game": 800000},
    ]

    def fetch(appid: int) -> PlayerCountResult:
        result = results[appid]
        if isinstance(result, Exception):
            raise result
        return result

    api.fetch_current_players.side_effect = fetch
    return api


def hourly_operator(active_ids: list[int], **kwargs: Any) -> SteamPlayerCountsToKafkaOperator:
    operator = SteamPlayerCountsToKafkaOperator(
        task_id="players",
        kafka_topic="steam.players",
        snapshot_id="20261122T030000",
        snapshot_prefix="players/dt=2026-11-22/hour=03",
        slot_at=str(SLOT_AT),
        rate_per_sec=1000.0,
        **kwargs,
    )
    operator._query_ids = MagicMock(return_value=active_ids)  # type: ignore[method-assign]
    return operator


def test_hourly_collects_top100_once_and_only_missing_active_games(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("time.time", lambda: SLOT_AT + 10)
    api = player_api(
        {
            10: PlayerCountResult(55, PLAYER_STATUS_OK),
            20: PlayerCountResult(None, PLAYER_STATUS_NOT_FOUND),
            30: requests.ConnectionError("down"),
        }
    )
    kafka = FakeKafkaHook()

    manifest = hourly_operator([730, 10, 20, 30]).publish(api, kafka)  # type: ignore[arg-type]

    called = sorted(call.args[0] for call in api.fetch_current_players.call_args_list)
    assert called == [10, 20, 30], "top 100에 있는 730은 개별 호출하지 않음"
    assert manifest["records"] == 5
    by_app = {message["appid"]: message for message in kafka.sent}
    assert by_app[730]["source"] == "top100" and by_app[10]["source"] == "individual"
    assert by_app[30]["status"] == PLAYER_STATUS_ERROR and by_app[30]["player_count"] is None
    assert {message["collected_at"] for message in kafka.sent} == {SLOT_AT}, "모두 같은 슬롯"


def test_hourly_rejects_slot_far_from_run_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("time.time", lambda: SLOT_AT + 2 * 3600)

    with pytest.raises(ValueError, match="허용 범위 밖"):
        hourly_operator([]).publish(player_api({}), FakeKafkaHook())  # type: ignore[arg-type]


def test_consecutive_failures_stop_before_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("time.time", lambda: SLOT_AT)
    failing = dict.fromkeys(range(1, 6), requests.Timeout("slow"))

    with pytest.raises(RuntimeError, match="연속 수집 실패"):
        hourly_operator(list(failing), max_consecutive_errors=3).publish(
            player_api(failing),  # type: ignore[arg-type]
            FakeKafkaHook(),  # type: ignore[arg-type]
        )


def test_scan_rejects_non_positive_limit() -> None:
    operator = SteamPlayerScanToKafkaOperator(
        task_id="scan", kafka_topic="t", snapshot_id="s", snapshot_prefix="p", max_targets="0"
    )

    with pytest.raises(ValueError, match="1 이상"):
        list(operator.iter_snapshot_pages(MagicMock(), SLOT_AT))


# ── Silver ──────────────────────────────────────


def test_empty_scan_snapshot_loads_without_parquet() -> None:
    s3 = MagicMock()
    s3.key_exists.return_value = True
    s3.read_json.return_value = {
        "source": "player_scan",
        "snapshot_id": "s",
        "collected_at": SLOT_AT,
        "records": 0,
        "api_total": None,
        "skipped": 0,
    }
    operator = SilverPlayerScanOperator(task_id="apply", snapshot_prefix="player_scan/s")

    manifest, snapshot = operator.load_snapshot(s3)
    summary = operator.apply_snapshot(MagicMock(), manifest, snapshot)

    assert snapshot.is_empty()
    s3.list_keys.assert_not_called()
    assert summary["scanned"] == 0
