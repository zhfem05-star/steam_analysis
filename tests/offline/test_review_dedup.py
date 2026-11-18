from unittest.mock import MagicMock
import polars as pl
import pytest
from utils.review_dedup import deduplicate_reviews
from operators.silver_reviews_to_s3 import SilverReviewsToS3Operator
from operators.silver_review_daily_to_fact import SilverReviewDailyToFactOperator
from operators.silver_review_playtime_to_fact import SilverReviewPlaytimeToFactOperator


def data():
    return pl.DataFrame([
        dict(appid=1,recommendationid="a",timestamp_created=100,timestamp_updated=100,
             _collected_at=1,language="english",review="beautiful scenery",author_playtime_at_review=10,author_playtime_forever=20),
        dict(appid=1,recommendationid="a",timestamp_created=100,timestamp_updated=200,
             _collected_at=2,language="english",review="excellent music",author_playtime_at_review=30,author_playtime_forever=40),
        dict(appid=1,recommendationid="b",timestamp_created=100,timestamp_updated=100,
             _collected_at=1,language="english",review="excellent music",author_playtime_at_review=50,author_playtime_forever=60),
    ])


def test_cross_file_dedup_keeps_latest_edit_and_is_idempotent():
    joined=pl.concat([data(),data().head(1),data()])
    result=deduplicate_reviews(joined)
    assert result.height == 2
    assert result.filter(pl.col("recommendationid")=="a")["review"][0] == "excellent music"
    assert deduplicate_reviews(result).equals(result)


def test_collection_time_breaks_equal_edit_ties_and_games_are_distinct():
    first=data().head(1)
    newer=first.with_columns(pl.lit(2).alias("_collected_at"),pl.lit(999).alias("author_playtime_forever"))
    other=first.with_columns(pl.lit(2).alias("appid"))
    result=deduplicate_reviews(pl.concat([newer,other,first],how="diagonal_relaxed"))
    assert result.height == 2
    assert result.filter(pl.col("appid")==1)["author_playtime_forever"][0] == 999


def test_missing_identity_fails_instead_of_collapsing_unrelated_reviews():
    with pytest.raises(ValueError):deduplicate_reviews(pl.DataFrame({"appid":[1]}))
    with pytest.raises(ValueError):deduplicate_reviews(pl.DataFrame({"appid":[1],"recommendationid":[None]}))


def test_daily_count_and_playtime_are_unbiased_by_duplicates():
    frame=pl.concat([data(),data().head(1),data()])
    counts=SilverReviewDailyToFactOperator._aggregate_daily(frame,1)
    playtime=SilverReviewPlaytimeToFactOperator._aggregate_daily(frame,1)
    assert counts[0]["review_count"] == 2
    assert playtime[0]["review_count"] == 2
    assert playtime[0]["avg_playtime_at_review"] == 40
    assert playtime[0]["avg_playtime_forever"] == 50


def test_silver_keys_do_not_overwrite_other_runs_and_are_repeatable():
    key=SilverReviewsToS3Operator._source_filename
    assert key("reviews/day1/1_chunk_000.parquet") != key("reviews/day2/1_chunk_000.parquet")
    assert key("reviews/day1/1_chunk_000.parquet") == key("reviews/day1/1_chunk_000.parquet")


def test_silver_transform_deduplicates_and_keeps_identity():
    transformed=SilverReviewsToS3Operator._transform(data())
    assert transformed.height == 2
    assert set(transformed["recommendationid"]) == {"a","b"}


def test_aggregate_replacement_uses_same_transaction():
    hook=MagicMock()
    conn=hook.get_conn.return_value
    cursor=conn.cursor.return_value.__enter__.return_value
    op=SilverReviewDailyToFactOperator(task_id="replace")
    rows=[dict(app_id=1,language="english",review_date="2026-01-01",review_count=2)]
    op._upsert(hook,rows)
    conn.__enter__.assert_called_once()
    assert "DELETE FROM fact_review_daily" in cursor.execute.call_args.args[0]
    assert cursor.execute.call_args.args[1] == (1,)
    cursor.executemany.assert_called_once()
