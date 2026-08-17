"""
Kafka 발행 여부와 전처리 여부, 커서 갱신 여부, 그리고 오퍼레이터 전체 로직을 확인하는 테스트 스크립트
"""
from unittest.mock import patch, MagicMock
from operators.steam_reviews_to_kafka import SteamReviewsToKafkaOperator


def test_execute_publishes_reviews_to_kafka(mock_kafka_hook, mock_pg_hook, dummy_context):
    fake_pages = [([{"recommendationid": "1", "review": "good"}], "cursor_A")]
    mock_api_hook = MagicMock()
    mock_api_hook.iter_review_pages.return_value = iter(fake_pages)

    with patch("operators.steam_reviews_to_kafka.SteamApiHook", return_value=mock_api_hook), \
         patch("operators.steam_reviews_to_kafka.SteamKafkaHook", return_value=mock_kafka_hook), \
         patch("operators.steam_reviews_to_kafka.PostgresHook", return_value=mock_pg_hook), \
         patch("operators.steam_reviews_to_kafka.slack_collect_summary"):

        op = SteamReviewsToKafkaOperator(
            task_id="test_task", kafka_topic="test-topic", app_ids=[123], languages=["korean"],
        )
        op.execute(dummy_context)

    # 발행 여부
    mock_kafka_hook.send.assert_called_once()

    # 전처리 여부 — appid/language가 원본 리뷰 dict에 병합됐는지
    _, kwargs = mock_kafka_hook.send.call_args
    assert kwargs["topic"] == "test-topic"
    assert kwargs["value"]["appid"] == 123
    assert kwargs["value"]["language"] == "korean"
    assert kwargs["value"]["recommendationid"] == "1"
    assert kwargs["key"] == "123"

    # flush 호출 여부
    mock_kafka_hook.flush.assert_called()

    # cursor 갱신(=발행 완료) 여부
    mock_pg_hook.run.assert_called()
