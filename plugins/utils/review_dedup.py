"""
여러 파일과 수집 실행에 중복된 리뷰의 최신본 선택
"""

import logging

import polars as pl

log = logging.getLogger(__name__)


def deduplicate_reviews(df: pl.DataFrame, app_id: int | None = None) -> pl.DataFrame:
    """
    게임·리뷰 식별 키 기준 최신 리뷰 선택

    :param df: 중복 제거 대상 리뷰
    :param app_id: appid 컬럼이 없을 때 사용할 게임 ID
    :return: 수정 시각과 수집 시각 기준 최신 리뷰
    :raises ValueError: 리뷰 식별 키 누락 또는 빈 값 포함
    """
    if df.is_empty():
        return df
    # 리뷰 식별 키 보완 및 무결성 검증
    if "appid" not in df.columns and app_id is not None:
        df = df.with_columns(pl.lit(app_id).alias("appid"))
    if not {"appid", "recommendationid"}.issubset(df.columns):
        raise ValueError(
            "리뷰 무결성 검증에 appid와 recommendationid 컬럼이 필요합니다"
        )
    df = df.with_columns(
        pl.col("appid").cast(pl.Int64),
        pl.col("recommendationid").cast(pl.String).str.strip_chars(),
    )
    invalid = df.filter(
        pl.col("appid").is_null()
        | pl.col("recommendationid").is_null()
        | (pl.col("recommendationid") == "")
    )
    if not invalid.is_empty():
        raise ValueError("리뷰 식별 키에 null 또는 빈 문자열이 있습니다")
    # Steam 수정 시각이 최신인 리뷰를 우선하고, 같으면 가장 최근 수집본을 선택한다.
    # 리뷰를 수정하지 않아도 플레이타임은 바뀔 수 있기 때문이다.
    # 수집 시각이 없는 예전 파일은 입력 파일 순서가 일정하도록 정렬한 뒤 마지막 행을 선택한다.
    order = [c for c in ("timestamp_updated", "_collected_at") if c in df.columns]
    if order:
        df = df.sort(order, nulls_last=False, maintain_order=True)
    result = df.unique(
        subset=["appid", "recommendationid"], keep="last", maintain_order=True
    )
    log.info(
        "리뷰 중복 제거: 입력=%d건, 고유=%d건, 중복=%d건",
        df.height,
        result.height,
        df.height - result.height,
    )
    return result
