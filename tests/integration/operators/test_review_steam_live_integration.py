"""실제 Steam API의 짧은 날짜 범위 조회 검증

RUN_STEAM_LIVE_TEST=1일 때만 실행하며 외부 저장소에는 기록하지 않음
"""

import json
import os
from datetime import datetime, timezone
from collections.abc import Callable
from typing import cast

import pytest

from hooks.steam_api import SteamApiHook
from utils.review_collection import ReviewCollector
from utils.review_types import WindowCheckpoint

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("RUN_STEAM_LIVE_TEST") != "1",
        reason="실제 Steam 호출은 RUN_STEAM_LIVE_TEST=1로 명시한 경우에만 실행",
    ),
]


def test_live_steam_date_range_completes(
    record_property: Callable[[str, object], None],
) -> None:
    """고정된 10분 구간의 날짜 경계·정렬·완료 상태 검증"""
    start = int(datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc).timestamp())
    end = start + 600
    collector = ReviewCollector(
        start_date=start,
        end_date=end,
        date_range_type="include",
    )
    ids: set[str] = set()
    timestamps: list[int] = []
    page_count = 0
    final: WindowCheckpoint | None = None
    pages = collector.pages(730, "english", None, SteamApiHook())
    try:
        for item in pages:
            final = cast(WindowCheckpoint, item.checkpoint)
            if not item.reviews:
                continue
            page_count += 1
            assert page_count <= 5, "소량 검증 한도인 5페이지를 초과했습니다"
            for review in item.reviews:
                timestamp = review["timestamp_created"]
                assert start <= timestamp <= end
                assert review["language"] == "english"
                ids.add(str(review["recommendationid"]))
                timestamps.append(timestamp)
    finally:
        pages.close()

    assert final is not None and final["completed"], "날짜 구간이 완료되지 않았습니다"
    assert timestamps, "응답이 비어 실제 리뷰의 날짜 필터를 검증할 수 없습니다"
    assert timestamps == sorted(timestamps, reverse=True)
    assert len(ids) == len(timestamps), "단일 날짜 구간에서 중복 리뷰가 반환됐습니다"
    summary = {
        "appid": 730,
        "language": "english",
        "filter": "recent",
        "purchase_type": "all",
        "start_date": start,
        "end_date": end,
        "review_pages": page_count,
        "rows": len(timestamps),
        "distinct_ids": len(ids),
        "earliest_timestamp": min(timestamps),
        "latest_timestamp": max(timestamps),
        "completed": final["completed"],
    }
    record_property("steam_live_summary", json.dumps(summary))
    print(json.dumps(summary))
