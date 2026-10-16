from copy import deepcopy
from dataclasses import replace
import pytest
from utils.review_collection import ReviewCollector


class API:
    def __init__(self, rows, page_size=2):
        self.rows = sorted(rows, key=lambda r: r["timestamp_created"], reverse=True)
        self.calls = []
        self.page_size = page_size

    def iter_review_pages(self, **kwargs):
        self.calls.append(kwargs)
        rows = [r for r in self.rows
                if kwargs["start_date"] <= r["timestamp_created"] <= kwargs["end_date"]]
        offset = 0 if kwargs["start_cursor"] == "*" else int(kwargs["start_cursor"])
        while offset < len(rows):
            page = rows[offset:offset+self.page_size]
            offset += len(page)
            yield page, str(offset)


def rows(*times):
    return [{"recommendationid": str(i), "timestamp_created": max(1, t)}
            for i, t in enumerate(times)]


def ids(items):
    return [r["recommendationid"] for item in items for r in item.reviews]


def collector(**kwargs):
    return ReviewCollector(start_date=1, end_date=100, date_range_type="include", **kwargs)


def test_rollover_replays_only_boundary_and_never_loses_reviews():
    api = API(rows(*range(0, 101, 10)))
    items = list(collector(review_limit=6).pages(1, "english", None, api))
    emitted = ids(items)
    assert set(emitted) == {r["recommendationid"] for r in api.rows}
    assert len(emitted) <= len(api.rows) + 2
    assert [c["end_date"] for c in api.calls] == [100, 51, 2]
    assert items[-1].checkpoint["completed"] is True


def test_restart_at_every_checkpoint_preserves_union():
    original = rows(*range(0, 101, 10))
    c = collector(review_limit=6)
    items = list(c.pages(1, "english", None, API(original)))
    for index, item in enumerate(items):
        saved = deepcopy(item.checkpoint)
        resumed = list(c.pages(1, "english", saved, API(original)))
        assert set(ids(items[:index+1]) + ids(resumed)) == {r["recommendationid"] for r in original}
        assert saved == item.checkpoint


def test_day_windows_share_boundary():
    c = replace(collector(window_days=1), end_date=3*86400)
    api = API(rows(0, 86400, 86401, 172800, 172801, 259200))
    items = list(c.pages(1, "english", None, api))
    assert [(c["start_date"], c["end_date"]) for c in api.calls] == [
        (172800,259200),(86400,172801),(1,86401)]
    assert set(ids(items)) == {r["recommendationid"] for r in api.rows}
    assert len(ids(items)) > len(api.rows)


def test_backfill_cutoff_frozen_across_later_runs_then_incremental():
    c = replace(collector(incremental_overlap_seconds=5), end_date=None)
    original = rows(0,20,50,80,100,110,120,130,150)
    iterator = c.continuous_pages(1, "english", None, API(original), 100)
    saved = next(iterator).checkpoint
    iterator.close()
    api = API(original)
    items = list(c.continuous_pages(1, "english", saved, api, 120))
    state = items[-1].checkpoint
    assert state["backfill"]["end_date"] == 100
    assert state["collected_through"] == 120
    assert api.calls[-1]["start_date"] == 95
    assert state["incremental"]["end_date"] == 120
    assert set(ids(items)) == {r["recommendationid"] for r in original if r["timestamp_created"] <= 120}
    api = API(original)
    later = list(c.continuous_pages(1, "english", state, api, 150))
    assert [(call["start_date"],call["end_date"]) for call in api.calls] == [(115,150)]
    assert later[-1].checkpoint["collected_through"] == 150


def test_unfinished_incremental_resumes_frozen_range_before_new_range():
    c = replace(collector(incremental_overlap_seconds=5), end_date=None)
    data = rows(0,90,100,105,110,115,120,130,140,150)
    complete = list(c.continuous_pages(1, "english", None, API(data), 100))[-1].checkpoint
    items = list(c.continuous_pages(1, "english", complete, API(data), 120))
    partial = next(x.checkpoint for x in items if x.reviews and x.checkpoint["incremental"])
    assert partial["collected_through"] == 100
    api = API(data)
    resumed = list(c.continuous_pages(1, "english", partial, api, 150))
    assert api.calls[0]["end_date"] == 120
    assert api.calls[0]["start_cursor"] != "*"
    assert api.calls[-1]["start_date"] == 115
    assert resumed[-1].checkpoint["collected_through"] == 150


def test_completed_same_cutoff_does_not_query_again():
    c = replace(collector(), end_date=None)
    state = list(c.continuous_pages(1, "english", None, API([]), 100))[-1].checkpoint
    api = API([])
    list(c.continuous_pages(1, "english", state, api, 100))
    assert api.calls == []


@pytest.mark.parametrize("bad", [
    [{"recommendationid":"1"}],
    [{"timestamp_created":101}],
    [{"timestamp_created":20},{"timestamp_created":30}],
])
def test_invalid_date_response_never_completes(bad):
    class BadAPI:
        def iter_review_pages(self, **kwargs):
            yield bad, "next"
    it = collector().pages(1,"english",None,BadAPI())
    assert not next(it).checkpoint["completed"]
    with pytest.raises(ValueError): list(it)


def test_dense_timestamp_fails_instead_of_silently_skipping():
    with pytest.raises(RuntimeError, match="날짜 경계를 이동할 수 없습니다"):
        list(collector(review_limit=4).pages(1,"english",None,API(rows(*([100]*10)))))


def test_legacy_date_checkpoint_restarts_and_cursor_only_preserves():
    assert collector().checkpoint("old")["cursor"] == "*"
    assert ReviewCollector().checkpoint("old")["cursor"] == "old"
    state = collector().checkpoint(None)
    state["version"] = 2
    state["cursor"] = "old"
    assert collector().checkpoint(state)["cursor"] == "*"


def test_zero_start_date_rejected_because_steam_ignores_filter():
    with pytest.raises(ValueError, match="start_date는 0보다 큰"):
        ReviewCollector(start_date=0,end_date=100,date_range_type="include")
