"""실제 Kafka·Postgres에서 체크포인트 재개를 검증한다. Steam 응답은 고정된 테스트 데이터로 대체한다."""
import json
import random
from unittest.mock import MagicMock
import pytest
from hooks.kafka_hook import SteamKafkaHook
from operators.steam_reviews_to_kafka import SteamReviewsToKafkaOperator

pytestmark = pytest.mark.integration


def test_checkpoint_resume_and_incremental_watermark(pg_hook):
    appid = random.randint(100_000_000, 999_999_999)
    pg_hook.run("INSERT INTO tracked_games(appid,first_seen,last_discounted) VALUES (%s,CURRENT_DATE,CURRENT_DATE)",parameters=(appid,))
    op=SteamReviewsToKafkaOperator(task_id="resume",app_ids=[appid],languages=["english"],
        collection_mode="continuous",date_range_type="include",start_date=1)
    op._run_end=100
    api=MagicMock()
    def failing(**kwargs):
        yield [{"recommendationid":"1","timestamp_created":90,"review":"first"}],"safe"
        raise RuntimeError("simulated interruption")
    api.iter_review_pages.side_effect=failing
    kafka=SteamKafkaHook()
    try:
        with pytest.raises(RuntimeError,match="simulated interruption"):
            op._collect_and_publish(appid,{},api,kafka,pg_hook)
        def state():
            value=pg_hook.get_first("SELECT review_cursors FROM tracked_games WHERE appid=%s",parameters=(appid,))[0]
            return json.loads(value) if isinstance(value,str) else value
        saved=state()
        assert saved["english"]["collected_through"] is None
        assert saved["english"]["backfill"]["cursor"] == "safe"
        assert not saved["english"]["backfill"]["completed"]
        assert op._fetch_targets() == [(appid,saved)]
        api.reset_mock()
        api.iter_review_pages.side_effect=lambda **kw: iter([])
        op._collect_and_publish(appid,saved,api,kafka,pg_hook)
        assert api.iter_review_pages.call_args.kwargs["start_cursor"] == "safe"
        assert state()["english"]["collected_through"] == 100
        op._run_end=200
        op._collect_and_publish(appid,state(),api,kafka,pg_hook)
        assert state()["english"]["collected_through"] == 200
        assert state()["english"]["backfill"]["end_date"] == 100
    finally:
        kafka.close()
        pg_hook.run("DELETE FROM tracked_games WHERE appid=%s",parameters=(appid,))
