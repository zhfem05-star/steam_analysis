import json
from datetime import datetime, timezone
from unittest.mock import MagicMock
import pytest
import operators.steam_reviews_to_kafka as mod
from hooks.kafka_hook import SteamKafkaHook
from hooks.steam_api import SteamApiHook


def delivery_hook(*futures):
    hook = SteamKafkaHook()
    hook._producer = MagicMock()
    hook._producer.send.side_effect = list(futures)
    return hook


def test_flush_requires_every_ack_and_keeps_failures_sticky():
    first, failed = MagicMock(), MagicMock()
    failed.get.side_effect = RuntimeError("delivery rejected")
    hook = delivery_hook(first, failed)
    hook.send("topic", {"id":1})
    hook.send("topic", {"id":2})
    with pytest.raises(RuntimeError, match="delivery rejected"):
        hook.flush()
    assert hook._pending == [first,failed]
    first.get.assert_called_once()
    failed.get.assert_called_once()


def test_successful_flush_clears_pending():
    future = MagicMock()
    hook = delivery_hook(future)
    hook.send("topic", {"id":1})
    hook.flush()
    future.get.assert_called_once()
    assert hook._pending == []


def test_failed_ack_cannot_advance_checkpoint_or_complete():
    future = MagicMock()
    future.get.side_effect = RuntimeError("async delivery failed")
    kafka = delivery_hook(future)
    api, pg = MagicMock(), MagicMock()
    api.iter_review_pages.return_value = iter([
        ([{"recommendationid":"1", "timestamp_created":50}], "unsafe")])
    op = mod.SteamReviewsToKafkaOperator(
        task_id="ack", languages=["english"], date_range_type="include", start_date=1, end_date=100)
    with pytest.raises(RuntimeError, match="async delivery"):
        op._collect_and_publish(1, {}, api, kafka, pg)
    saved = [json.loads(c.kwargs["parameters"][0])["english"] for c in pg.run.call_args_list]
    assert saved
    assert all(s["cursor"] == "*" and not s["completed"] for s in saved)


def test_api_failure_preserves_last_delivered_page_not_completion():
    def pages(**kwargs):
        yield [{"recommendationid":"1", "timestamp_created":50}], "safe"
        raise RuntimeError("network")
    api, kafka, pg = MagicMock(), MagicMock(), MagicMock()
    api.iter_review_pages.side_effect = pages
    op = mod.SteamReviewsToKafkaOperator(
        task_id="api_error", languages=["english"], date_range_type="include",start_date=1,end_date=100)
    with pytest.raises(RuntimeError, match="network"):
        op._collect_and_publish(1, {}, api, kafka, pg)
    saved = json.loads(pg.run.call_args.kwargs["parameters"][0])["english"]
    assert saved["cursor"] == "safe"
    assert not saved["completed"]


def test_consumer_failure_does_not_block_full_queue():
    import threading
    api, kafka, pg = MagicMock(), MagicMock(), MagicMock()
    api.iter_review_pages.return_value = (([{"recommendationid":str(i)}],str(i)) for i in range(100))
    kafka.send.side_effect = RuntimeError("send failed")
    op = mod.SteamReviewsToKafkaOperator(task_id="cancel",languages=["english"])
    failures = []
    def run():
        try: op._collect_and_publish(1, {}, api, kafka, pg)
        except Exception as error: failures.append(error)
    thread = threading.Thread(target=run,daemon=True)
    thread.start(); thread.join(timeout=3)
    assert not thread.is_alive(), "Producer is stuck on full queue"
    assert failures and isinstance(failures[0],RuntimeError)
    pg.run.assert_not_called()


def test_one_failed_app_fails_task_and_keeps_other_app_progress(monkeypatch):
    op = mod.SteamReviewsToKafkaOperator(task_id="partial")
    monkeypatch.setattr(op,"_fetch_targets",lambda:[(1,{}),(2,{})])
    monkeypatch.setattr(mod,"SteamApiHook",MagicMock())
    monkeypatch.setattr(mod,"SteamKafkaHook",MagicMock())
    monkeypatch.setattr(mod,"PostgresHook",MagicMock())
    summary = MagicMock()
    monkeypatch.setattr(mod,"slack_collect_summary",summary)
    def collect(appid,*args):
        if appid == 1: raise RuntimeError("failure")
    monkeypatch.setattr(op,"_collect_and_publish",collect)
    with pytest.raises(RuntimeError): op.execute({"ti":MagicMock()})
    assert summary.call_args.kwargs["succeeded"] == [2]
    assert summary.call_args.kwargs["failed"] == [1]


def test_continuous_mode_uses_airflow_interval_and_db_state(monkeypatch):
    op = mod.SteamReviewsToKafkaOperator(task_id="continuous",collection_mode="continuous",
        date_range_type="include",start_date=1,app_ids=[1])
    pg = MagicMock(); pg.get_records.return_value=[(1,{"english":{"version":3}})]
    monkeypatch.setattr(mod,"PostgresHook",lambda **kw:pg)
    assert op._fetch_targets() == [(1,{"english":{"version":3}})]
    assert pg.get_records.call_args.kwargs["parameters"] == ([1],)
    monkeypatch.setattr(op,"_fetch_targets",lambda:[])
    with pytest.raises(ValueError):
        op.execute({"data_interval_end":datetime.fromtimestamp(100,tz=timezone.utc)})
    assert op._run_end == 100


@pytest.mark.parametrize("response", [
    {"success":0}, {"success":1}, {"success":1,"reviews":{}},
    {"success":1,"reviews":[{"recommendationid":"1"}],"cursor":"*"},
])
def test_bad_hook_responses_are_errors_not_empty_completion(response):
    hook=SteamApiHook();hook._request=MagicMock(return_value=response);hook._wait=MagicMock()
    with pytest.raises((ValueError,RuntimeError)):
        list(hook.iter_review_pages(1,"english"))


@pytest.mark.parametrize("kwargs",[
    {"start_date":1},{"window_days":0},
    {"date_range_type":"include","start_date":0,"end_date":100},
    {"date_range_type":"include","start_date":10,"end_date":1},
    {"collection_mode":"continuous","date_range_type":"include","start_date":0,"end_date":1},
])
def test_invalid_collection_config(kwargs):
    with pytest.raises(ValueError):mod.SteamReviewsToKafkaOperator(task_id="invalid",**kwargs)


def test_empty_api_window_is_also_rate_limited():
    hook=SteamApiHook()
    hook._wait=MagicMock()
    hook._request=MagicMock(return_value={"success":1,"reviews":[]})
    assert list(hook.iter_review_pages(1,"english")) == []
    hook._wait.assert_called_once()


def test_existing_publish_regression():
    import importlib.util
    from pathlib import Path
    path=Path(__file__).parents[1]/"plugins/operators/test_steam_reviews_ETL.py"
    spec=importlib.util.spec_from_file_location("legacy_review_test",path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.test_execute_publishes_reviews_to_kafka(MagicMock(),MagicMock(),{"ti":MagicMock()})
