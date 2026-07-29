"""
S3 저장 여부와 전처리 여부, 커서 갱신 여부, 그리고 오퍼레이터 전체 로직을 확인하는 테스트 스크립트
"""
from unittest.mock import patch, MagicMock
from operators.steam_reviews_to_s3 import SteamReviewsToS3Operator


def test_execute_uploads_chunk_with_appid_column(mock_s3_hook, mock_pg_hook, dummy_context):
    fake_pages = [([{"recommendationid": "1", "review": "good"}], "cursor_A")]
    mock_api_hook = MagicMock()
    mock_api_hook.iter_review_pages.return_value = iter(fake_pages)

    with patch("operators.steam_reviews_to_s3.SteamApiHook", return_value=mock_api_hook), \
         patch("operators.steam_reviews_to_s3.SteamS3Hook", return_value=mock_s3_hook), \
         patch("operators.steam_reviews_to_s3.PostgresHook", return_value=mock_pg_hook), \
         patch("operators.steam_reviews_to_s3.slack_collect_summary"):

        op = SteamReviewsToS3Operator(
            task_id="test_task", s3_key_prefix="test_prefix", app_ids=[123], languages=["korean"],
        )
        op.execute(dummy_context)

    # 저장 여부
    mock_s3_hook.upload_parquet.assert_called_once()
    saved_df = mock_s3_hook.upload_parquet.call_args.kwargs["df"]

    # 전처리 여부
    assert "appid" in saved_df.columns
    assert saved_df["appid"][0] == 123

    # cursor 갱신(=저장 완료) 여부
    mock_pg_hook.run.assert_called()