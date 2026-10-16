"""
날짜 구간 순회 및 과거·증분 수집의 체크포인트 관리
"""

from __future__ import annotations

from collections.abc import Iterator
from copy import deepcopy
from dataclasses import dataclass
from typing import NamedTuple, cast

from utils.review_types import (
    Checkpoint,
    ContinuousCheckpoint,
    ContinuousConfig,
    ReviewPageSource,
    ReviewRecord,
    SavedCheckpoint,
    WindowCheckpoint,
)


class ReviewPage(NamedTuple):
    """
    한 페이지의 리뷰 및 발행 성공 후 저장할 체크포인트

    빈 리뷰 목록: 날짜 구간 전환 또는 수집 완료 신호
    """

    language: str
    reviews: list[ReviewRecord]
    checkpoint: Checkpoint


@dataclass(frozen=True)
class ReviewCollector:
    """
    날짜 경계 중첩 수집 및 페이지별 재시작 상태 관리
    """

    start_date: int | None = None
    end_date: int | None = None
    filter_type: str = "recent"
    date_range_type: str | None = None
    window_days: int = 30
    review_limit: int = 50000
    overlap_seconds: int = 1
    incremental_overlap_seconds: int = 86400

    def __post_init__(self) -> None:
        if self.start_date is not None and self.start_date <= 0:
            raise ValueError(
                "start_date는 0보다 큰 Unix timestamp여야 합니다: "
                f"start_date={self.start_date!r}"
            )

    # ── 체크포인트 복원 ─────────────────────────────────────────

    def checkpoint(
        self,
        saved: SavedCheckpoint,
        start: int | None = None,
        end: int | None = None,
    ) -> WindowCheckpoint:
        """
        조회 조건에 따른 체크포인트 복원 또는 초기화

        :param saved: 이전 실행에서 저장한 체크포인트 또는 구버전 cursor
        :param start: 수집 시작 시각 재정의 (초 단위 Unix timestamp)
        :param end: 수집 종료 시각 재정의 (초 단위 Unix timestamp)
        :return: 조회 조건이 일치하면 기존 상태, 불일치하면 초기 상태
        """
        start = self.start_date if start is None else start
        end = self.end_date if end is None else end
        state: WindowCheckpoint = dict(
            cursor="*",
            filter_type=self.filter_type,
            date_range_type=self.date_range_type,
            start_date=start,
            end_date=end,
        )
        # cursor를 제외한 조회 조건이 같을 때만 기존 진행 상태 복원
        conditions = list(state.keys())[1:]
        if start is not None:
            state.update(
                {
                    "version": 3,
                    "window_days": self.window_days,
                    "overlap_seconds": self.overlap_seconds,
                    "window_start": max(
                        start, cast(int, end) - self.window_days * 86400
                    ),
                    "window_end": cast(int, end),
                    "window_count": 0,
                    "oldest_timestamp": None,
                    "completed": False,
                }
            )
            conditions += ["version", "window_days", "overlap_seconds"]

        saved_conditions_match = isinstance(saved, dict) and all(
            saved.get(key) == cast(dict[str, object], state)[key] for key in conditions
        )
        if saved_conditions_match:
            state = cast(WindowCheckpoint, deepcopy(saved))
        elif isinstance(saved, str) and start is None:
            state["cursor"] = saved

        return state

    # ── 고정 날짜 구간 수집 ─────────────────────────────────────

    def pages(
        self,
        appid: int,
        language: str,
        saved: SavedCheckpoint,
        api: ReviewPageSource,
        start: int | None = None,
        end: int | None = None,
    ) -> Iterator[ReviewPage]:
        """
        고정 날짜 범위의 최신순 페이지 수집

        :param appid: 수집 대상 게임 ID
        :param language: 수집 언어
        :param saved: 이전 실행의 체크포인트
        :param api: 리뷰 페이지 조회용 API
        :param start: 수집 시작 시각 재정의 (초 단위 Unix timestamp)
        :param end: 수집 종료 시각 재정의 (초 단위 Unix timestamp)
        :yield: 리뷰 페이지 및 발행 성공 후 저장할 체크포인트
        """
        state = self.checkpoint(saved, start, end)
        dated = state["start_date"] is not None
        if dated and state["completed"]:
            return

        # 첫 API 요청이 실패해도 같은 날짜 범위로 재개하도록 시작 상태를 먼저 전달한다.
        if dated:
            yield ReviewPage(language, [], deepcopy(state))

        while True:
            rollover = dated and state["window_count"] >= self.review_limit
            # 건수 상한에 도달한 구간은 API 요청 없이 날짜 경계부터 이동
            pages = (
                iter(())
                if rollover
                else api.iter_review_pages(
                    appid=appid,
                    language=language,
                    start_cursor=state["cursor"],
                    filter_type=self.filter_type,
                    date_range_type=self.date_range_type,
                    start_date=state["window_start"] if dated else None,
                    end_date=state["window_end"] if dated else None,
                )
            )
            try:
                for reviews, cursor in pages:
                    has_missing_review_id = any(
                        not str(review.get("recommendationid") or "").strip()
                        for review in reviews
                    )
                    if has_missing_review_id:
                        raise ValueError(
                            "Steam 응답에 recommendationid가 없는 리뷰가 있습니다: "
                            f"appid={appid}, language={language}"
                        )

                    if dated:
                        timestamps = self._validate_review_timestamps(reviews, state)
                        state = {
                            **state,
                            "oldest_timestamp": min(timestamps),
                            "window_count": state["window_count"] + len(reviews),
                        }

                    state = {**state, "cursor": cursor}
                    yield ReviewPage(language, reviews, deepcopy(state))

                    if dated and state["window_count"] >= self.review_limit:
                        rollover = True
                        break
            finally:
                if hasattr(pages, "close"):
                    pages.close()

            if not dated:
                return

            # 현재 구간 종료 후 다음 날짜 경계 계산
            low, high = state["window_start"], state["window_end"]
            if rollover:
                # 최신순으로 읽었으므로 마지막 리뷰보다 새로운 시각은 이미 순회했다.
                # 같은 초에 작성된 다른 리뷰가 빠지지 않도록 경계 시각을 겹쳐 다시 읽는다.
                next_high = cast(int, state["oldest_timestamp"]) + self.overlap_seconds
                if next_high >= high:
                    raise RuntimeError(
                        "동일 시각에 리뷰가 집중되어 날짜 경계를 이동할 수 없습니다: "
                        f"appid={appid}, language={language}, "
                        f"현재 종료={high}, 다음 종료={next_high}, "
                        f"구간 수집 수={state['window_count']}"
                    )
                high = next_high
            elif low > cast(int, state["start_date"]):
                high = low + self.overlap_seconds
                low = max(
                    cast(int, state["start_date"]), low - self.window_days * 86400
                )
            else:
                yield ReviewPage(language, [], {**state, "completed": True})
                return

            state = {
                **state,
                "cursor": "*",
                "window_start": low,
                "window_end": high,
                "window_count": 0,
                "oldest_timestamp": None,
            }
            yield ReviewPage(language, [], deepcopy(state))

    # ── 페이지 무결성 검증 ──────────────────────────────────────

    @staticmethod
    def _validate_review_timestamps(
        reviews: list[ReviewRecord],
        state: WindowCheckpoint,
    ) -> list[int]:
        """
        날짜 구간과 최신순 정렬을 검증한 작성 시각 목록 반환

        :param reviews: Steam API에서 받은 한 페이지의 리뷰
        :param state: 요청한 날짜 구간과 직전 페이지의 진행 상태
        :return: 초 단위 Unix timestamp 목록
        :raises ValueError: 작성 시각 누락, 범위 이탈 또는 최신순 정렬 위반
        """
        raw_timestamps: list[object] = [
            review.get("timestamp_created") for review in reviews
        ]
        if not raw_timestamps or any(
            type(value) is not int for value in raw_timestamps
        ):
            raise ValueError(
                "날짜 수집 리뷰의 timestamp_created는 정수여야 합니다: "
                f"리뷰 수={len(reviews)}, 작성 시각={raw_timestamps!r}"
            )

        # API 원본 값을 검증한 뒤 정수 목록으로 사용
        timestamps = cast(list[int], raw_timestamps)
        earliest_timestamp = min(timestamps)
        latest_timestamp = max(timestamps)
        window_start = state["window_start"]
        window_end = state["window_end"]

        if earliest_timestamp < window_start or latest_timestamp > window_end:
            raise ValueError(
                "Steam 응답의 작성 시각이 요청한 날짜 범위를 벗어났습니다: "
                f"요청=[{window_start}, {window_end}], "
                f"응답=[{earliest_timestamp}, {latest_timestamp}]"
            )

        previous_oldest = state["oldest_timestamp"]
        page_is_descending = timestamps == sorted(timestamps, reverse=True)
        follows_previous_page = (
            previous_oldest is None or latest_timestamp <= previous_oldest
        )
        if not page_is_descending or not follows_previous_page:
            raise ValueError(
                "Steam recent 응답이 작성 시각 내림차순이 아닙니다: "
                f"페이지 정렬 정상={page_is_descending}, "
                f"이전 페이지 최저 시각={previous_oldest}, "
                f"현재 페이지 최고 시각={latest_timestamp}"
            )

        return timestamps

    # ── 과거 수집 및 증분 수집 ──────────────────────────────────

    def continuous_pages(
        self,
        appid: int,
        language: str,
        saved: SavedCheckpoint,
        api: ReviewPageSource,
        cutoff: int,
    ) -> Iterator[ReviewPage]:
        """
        과거 수집 완료 후 실행 종료 시각까지 증분 수집

        :param appid: 수집 대상 게임 ID
        :param language: 수집 언어
        :param saved: 이전 실행의 체크포인트
        :param api: 리뷰 페이지 조회용 API
        :param cutoff: 이번 실행의 수집 종료 시각 (초 단위 Unix timestamp)
        :yield: 리뷰 페이지 및 과거·증분 수집 체크포인트
        """
        config: ContinuousConfig = dict(
            version=3,
            mode="continuous",
            start_date=cast(int, self.start_date),
            filter_type=self.filter_type,
            date_range_type=self.date_range_type,
            window_days=self.window_days,
            overlap_seconds=self.overlap_seconds,
            incremental_overlap_seconds=self.incremental_overlap_seconds,
        )
        if isinstance(saved, dict) and all(
            saved.get(k) == v for k, v in config.items()
        ):
            state = cast(ContinuousCheckpoint, deepcopy(saved))
        else:
            state = {
                **config,
                "backfill": self.checkpoint(None, self.start_date, cutoff),
                "incremental": None,
                "collected_through": None,
            }
        yield ReviewPage(language, [], deepcopy(state))

        # 최초 실행에서 고정한 과거 구간부터 완료
        if not state["backfill"]["completed"]:
            backfill = state["backfill"]
            for item in self.pages(
                appid,
                language,
                backfill,
                api,
                backfill["start_date"],
                backfill["end_date"],
            ):
                window_state = cast(WindowCheckpoint, item.checkpoint)
                state = {**state, "backfill": window_state}
                if window_state["completed"]:
                    state["collected_through"] = window_state["end_date"]
                yield ReviewPage(language, item.reviews, deepcopy(state))

        # 과거 수집 완료 시각부터 이번 실행의 종료 시각까지 증분 수집
        while True:
            active = state["incremental"]
            if active is None or active["completed"]:
                if cutoff <= cast(int, state["collected_through"]):
                    return
                lower = max(
                    cast(int, self.start_date),
                    cast(int, state["collected_through"])
                    - self.incremental_overlap_seconds,
                )
                active = self.checkpoint(None, lower, cutoff)
            # 다음 DAG 실행의 종료 시각이 더 늦어도, 중단된 원래 구간부터 끝낸다.
            # 해당 구간을 모두 처리한 뒤에만 수집 완료 시각을 전진시킨다.
            for item in self.pages(
                appid, language, active, api, active["start_date"], active["end_date"]
            ):
                window_state = cast(WindowCheckpoint, item.checkpoint)
                state = {**state, "incremental": window_state}
                if window_state["completed"]:
                    state["collected_through"] = window_state["end_date"]
                yield ReviewPage(language, item.reviews, deepcopy(state))
