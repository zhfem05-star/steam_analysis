"""
GetNumberOfCurrentPlayers rate limit 측정 스크립트

- 요청 간격과 동시 요청 수 조합별로 첫 429 발생 시점 측정
- 낮은 요청률 시나리오부터 실행하고 429 누적 시 시나리오 조기 종료
- 측정 결과로 시간당 요청 예산 산정

사용 예:
    python scripts/probe_current_players_rate_limit.py --output result.json
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import requests

log = logging.getLogger(__name__)

_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
_TIMEOUT_SEC = 10.0
_DEFAULT_APPID = 730


# ── 데이터 구조 ──────────────────────────────────────


@dataclass(frozen=True)
class Scenario:
    """
    측정 시나리오

    :param interval_sec: worker별 요청 간격
    :param concurrency: 동시 worker 수
    """

    interval_sec: float
    concurrency: int

    @property
    def target_rps(self) -> float:
        """목표 초당 요청 수"""
        return self.concurrency / self.interval_sec


@dataclass(frozen=True)
class RequestSample:
    """
    단일 요청 결과

    :param elapsed_sec: 시나리오 시작 기준 요청 시작 시점
    :param status: HTTP status, 네트워크 오류는 0
    :param latency_sec: 응답 소요 시간
    :param retry_after: Retry-After 헤더 값
    """

    elapsed_sec: float
    status: int
    latency_sec: float
    retry_after: str | None


@dataclass
class ScenarioResult:
    """시나리오 측정 결과 요약"""

    interval_sec: float
    concurrency: int
    target_rps: float
    duration_sec: float = 0.0
    total_requests: int = 0
    ok_count: int = 0
    rate_limited_count: int = 0
    error_count: int = 0
    actual_rps: float = 0.0
    first_429_elapsed_sec: float | None = None
    requests_before_first_429: int | None = None
    retry_after_values: list[str] = field(default_factory=list)
    stopped_early: bool = False


# ── 측정 실행 ──────────────────────────────────────


def _worker(
    scenario: Scenario,
    appid: int,
    started_at: float,
    deadline: float,
    stop_event: threading.Event,
    samples: list[RequestSample],
    lock: threading.Lock,
    max_429: int,
) -> None:
    """
    고정 간격으로 요청을 반복하는 worker

    - 요청 소요 시간을 간격에서 차감해 worker별 요청률 유지
    - 429 누적 수가 max_429에 도달하면 전체 worker 중단 신호 설정
    """
    session = requests.Session()

    while not stop_event.is_set() and time.monotonic() < deadline:
        request_started = time.monotonic()
        retry_after: str | None = None

        try:
            resp = session.get(_URL, params={"appid": appid}, timeout=_TIMEOUT_SEC)
            status = resp.status_code
            retry_after = resp.headers.get("Retry-After")
        except requests.RequestException:
            log.warning("요청 실패: appid=%s", appid, exc_info=True)
            status = 0

        latency = time.monotonic() - request_started
        sample = RequestSample(
            elapsed_sec=round(request_started - started_at, 3),
            status=status,
            latency_sec=round(latency, 3),
            retry_after=retry_after,
        )

        with lock:
            samples.append(sample)
            rate_limited_total = sum(1 for s in samples if s.status == 429)
        if rate_limited_total >= max_429:
            stop_event.set()
            break

        time.sleep(max(0.0, scenario.interval_sec - latency))


def run_scenario(
    scenario: Scenario,
    appid: int,
    duration_sec: float,
    max_429: int,
) -> ScenarioResult:
    """
    단일 시나리오 측정

    :param scenario: 요청 간격과 동시 요청 수
    :param appid: 조회 대상 게임 ID
    :param duration_sec: 시나리오 최대 실행 시간
    :param max_429: 조기 종료 기준 429 누적 수
    :return: 시나리오 측정 요약
    """
    samples: list[RequestSample] = []
    lock = threading.Lock()
    stop_event = threading.Event()
    started_at = time.monotonic()
    deadline = started_at + duration_sec

    threads = [
        threading.Thread(
            target=_worker,
            args=(
                scenario,
                appid,
                started_at,
                deadline,
                stop_event,
                samples,
                lock,
                max_429,
            ),
            daemon=True,
        )
        for _ in range(scenario.concurrency)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    return summarize(
        scenario, samples, time.monotonic() - started_at, stop_event.is_set()
    )


# ── 결과 변환 ──────────────────────────────────────


def summarize(
    scenario: Scenario,
    samples: list[RequestSample],
    duration_sec: float,
    stopped_early: bool,
) -> ScenarioResult:
    """
    요청 샘플을 시나리오 요약으로 변환

    - requests_before_first_429는 첫 429 이전 시작된 성공 요청 수
    """
    ordered = sorted(samples, key=lambda s: s.elapsed_sec)
    rate_limited = [s for s in ordered if s.status == 429]
    first_429 = rate_limited[0] if rate_limited else None

    result = ScenarioResult(
        interval_sec=scenario.interval_sec,
        concurrency=scenario.concurrency,
        target_rps=round(scenario.target_rps, 2),
        duration_sec=round(duration_sec, 2),
        total_requests=len(ordered),
        ok_count=sum(1 for s in ordered if s.status == 200),
        rate_limited_count=len(rate_limited),
        error_count=sum(1 for s in ordered if s.status not in (200, 429)),
        actual_rps=round(len(ordered) / duration_sec, 2) if duration_sec > 0 else 0.0,
        retry_after_values=sorted(
            {s.retry_after for s in rate_limited if s.retry_after}
        ),
        stopped_early=stopped_early,
    )
    if first_429 is not None:
        result.first_429_elapsed_sec = first_429.elapsed_sec
        result.requests_before_first_429 = sum(
            1
            for s in ordered
            if s.status == 200 and s.elapsed_sec < first_429.elapsed_sec
        )
    return result


def estimate_hourly_budget(results: list[ScenarioResult]) -> dict[str, float | None]:
    """
    시간당 요청 예산 산정

    - 429 없이 끝난 시나리오 중 최대 실측 요청률을 안전 상한으로 사용
    - 운영 예산은 안전 상한의 70%로 여유분 확보

    :param results: 시나리오 측정 결과 목록
    :return: 안전 상한 rps, 시간당 상한, 권장 시간당 예산
    """
    clean = [r for r in results if r.rate_limited_count == 0 and r.error_count == 0]
    if not clean:
        return {
            "max_clean_rps": None,
            "hourly_ceiling": None,
            "hourly_budget_70pct": None,
        }

    max_clean_rps = max(r.actual_rps for r in clean)
    return {
        "max_clean_rps": max_clean_rps,
        "hourly_ceiling": round(max_clean_rps * 3600),
        "hourly_budget_70pct": round(max_clean_rps * 3600 * 0.7),
    }


# ── 진입점 ──────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--appid", type=int, default=_DEFAULT_APPID)
    parser.add_argument("--intervals", type=float, nargs="+", default=[1.0, 0.5, 0.2])
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument(
        "--duration", type=float, default=60.0, help="시나리오별 최대 실행 시간(초)"
    )
    parser.add_argument(
        "--max-429", type=int, default=5, help="조기 종료 기준 429 누적 수"
    )
    parser.add_argument(
        "--cooldown",
        type=float,
        default=15.0,
        help="429 미발생 시 시나리오 간 대기(초)",
    )
    parser.add_argument(
        "--cooldown-after-429", type=float, default=120.0, help="429 발생 후 대기(초)"
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    """시나리오를 낮은 요청률 순으로 실행하고 결과를 JSON으로 저장"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = _parse_args()

    scenarios = sorted(
        (
            Scenario(interval, conc)
            for interval in args.intervals
            for conc in args.concurrency
        ),
        key=lambda s: s.target_rps,
    )
    results: list[ScenarioResult] = []

    for index, scenario in enumerate(scenarios):
        log.info(
            "시나리오 시작: interval=%.1fs concurrency=%d target_rps=%.1f",
            scenario.interval_sec,
            scenario.concurrency,
            scenario.target_rps,
        )
        result = run_scenario(scenario, args.appid, args.duration, args.max_429)
        results.append(result)
        log.info("시나리오 결과: %s", asdict(result))

        is_last = index == len(scenarios) - 1
        if not is_last:
            time.sleep(
                args.cooldown_after_429 if result.rate_limited_count else args.cooldown
            )

    report = {
        "appid": args.appid,
        "results": [asdict(r) for r in results],
        "budget": estimate_hourly_budget(results),
    }
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("결과 저장: %s budget=%s", args.output, report["budget"])


if __name__ == "__main__":
    main()
