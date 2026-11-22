"""
동접자 API 요청률 제어

- 모든 개별 동접자 호출이 하나의 token bucket을 공유해 초당 요청 수 상한 유지
- 429 응답 시 Retry-After만큼 대기하고 요청률을 낮춘 뒤 같은 요청 재시도
- 연속 429가 한도를 넘으면 IP 차단을 피하기 위해 실행 중단

기본 초당 5건은 2026-11-18 측정 무오류 상한(초당 40건)의 1/8
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")

# Retry-After 헤더가 없을 때 대기 시간
DEFAULT_RETRY_AFTER_SEC = 60.0


class SteamRateLimitError(Exception):
    """
    Steam API 429 응답

    :param retry_after: 응답 헤더 Retry-After 초 (없으면 None)
    """

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class TokenBucket:
    """
    초당 요청 수 제한용 token bucket

    :param rate_per_sec: 초당 허용 요청 수
    :param burst: 한 번에 몰아 쓸 수 있는 최대 token 수
    :param min_rate_per_sec: 요청률 감소 하한
    :param clock: 단조 증가 시계 (테스트 대체용)
    :param sleep: 대기 함수 (테스트 대체용)
    """

    def __init__(
        self,
        rate_per_sec: float,
        burst: int = 1,
        min_rate_per_sec: float = 0.5,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_per_sec <= 0:
            raise ValueError(f"요청률은 0보다 커야 합니다: rate_per_sec={rate_per_sec!r}")
        if burst < 1:
            raise ValueError(f"burst는 1 이상이어야 합니다: burst={burst!r}")

        self.rate_per_sec = rate_per_sec
        self.burst = burst
        self.min_rate_per_sec = min(min_rate_per_sec, rate_per_sec)
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(burst)
        self._updated_at = clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._updated_at
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate_per_sec)
        self._updated_at = now

    def acquire(self) -> None:
        """token 1개를 얻을 때까지 대기"""
        self._refill()
        if self._tokens < 1:
            self._sleep((1 - self._tokens) / self.rate_per_sec)
            self._refill()
        self._tokens -= 1

    def slow_down(self, factor: float = 0.5) -> None:
        """요청률을 factor배로 낮춤 (하한 min_rate_per_sec)"""
        self.rate_per_sec = max(self.min_rate_per_sec, self.rate_per_sec * factor)
        log.warning("요청률 감소: 초당 %.2f건", self.rate_per_sec)

    def pause(self, seconds: float) -> None:
        """서버가 요구한 시간만큼 대기하고 쌓인 token을 비움"""
        self._sleep(seconds)
        self._tokens = 0.0
        self._updated_at = self._clock()


class RateLimitedCaller:
    """
    token bucket을 거쳐 호출하고 429에 대응하는 실행기

    :param bucket: 공유 token bucket
    :param max_consecutive_throttles: 허용 연속 429 횟수. 초과 시 중단
    """

    def __init__(self, bucket: TokenBucket, max_consecutive_throttles: int = 3) -> None:
        self.bucket = bucket
        self.max_consecutive_throttles = max_consecutive_throttles
        self.calls = 0
        self.throttled = 0

    def call(self, func: Callable[[], T]) -> T:
        """
        요청률 제한을 지켜 func 실행

        :raises RuntimeError: 연속 429가 max_consecutive_throttles 초과
        """
        consecutive = 0
        while True:
            self.bucket.acquire()
            self.calls += 1
            try:
                return func()
            except SteamRateLimitError as error:
                consecutive += 1
                self.throttled += 1
                if consecutive > self.max_consecutive_throttles:
                    raise RuntimeError(
                        f"연속 429 {consecutive}회로 동접자 수집 중단: "
                        f"현재 요청률 초당 {self.bucket.rate_per_sec:.2f}건"
                    ) from error

                wait_sec = error.retry_after or DEFAULT_RETRY_AFTER_SEC
                log.warning(
                    "429 수신, %.0f초 대기 후 재시도 (연속 %d회)", wait_sec, consecutive
                )
                self.bucket.pause(wait_sec)
                self.bucket.slow_down()
