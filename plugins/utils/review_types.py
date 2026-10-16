"""
리뷰 수집용 API 응답 및 체크포인트 타입 정의
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Literal, Protocol, TypeAlias, TypedDict

# Steam 응답은 author 등 중첩 필드를 포함한다. 원본을 보존하므로 값의 타입은 열어 둔다.
ReviewRecord: TypeAlias = dict[str, Any]


class CursorCheckpoint(TypedDict):
    """
    날짜 필터 유무와 관계없이 저장하는 공통 조회 조건 및 cursor
    """

    cursor: str
    filter_type: str
    date_range_type: str | None
    start_date: int | None
    end_date: int | None


class WindowCheckpoint(CursorCheckpoint, total=False):
    """
    날짜 구간별 수집 진행 상태

    시각 필드: 초 단위 Unix timestamp
    """

    version: int
    window_days: int
    overlap_seconds: int
    window_start: int
    window_end: int
    window_count: int
    oldest_timestamp: int | None
    completed: bool


class ContinuousConfig(TypedDict):
    """
    과거·증분 수집 재개 시 일치 여부를 확인할 설정
    """

    version: int
    mode: Literal["continuous"]
    start_date: int
    filter_type: str
    date_range_type: str | None
    window_days: int
    overlap_seconds: int
    incremental_overlap_seconds: int


class ContinuousCheckpoint(ContinuousConfig):
    """
    최초 과거 구간·진행 중인 증분 구간·전체 수집 완료 시각
    """

    backfill: WindowCheckpoint
    incremental: WindowCheckpoint | None
    collected_through: int | None


Checkpoint: TypeAlias = WindowCheckpoint | ContinuousCheckpoint
# 문자열은 구버전 cursor, None은 해당 언어를 아직 수집하지 않은 상태다.
SavedCheckpoint: TypeAlias = str | Checkpoint | None
LanguageCheckpoints: TypeAlias = dict[str, str | Checkpoint]
CollectionMode: TypeAlias = Literal["fixed", "continuous"]


class ReviewPageSource(Protocol):
    """
    실제 Hook과 테스트용 API에서 구현할 수집 인터페이스
    """

    def iter_review_pages(
        self,
        appid: int,
        language: str,
        start_cursor: str = "*",
        filter_type: str = "recent",
        date_range_type: str | None = None,
        start_date: int | None = None,
        end_date: int | None = None,
    ) -> Iterator[tuple[list[ReviewRecord], str]]:
        """
        리뷰 페이지와 다음 요청용 cursor의 순차 반환

        :yield: (리뷰 목록, 다음 cursor)
        """
        ...
