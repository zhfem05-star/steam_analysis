"""
Gold 리뷰 형태소 빈도 적재 DAG

Silver S3 리뷰 parquet → gold_review_morphemes UPSERT
워드클라우드 시각화를 위한 게임별·언어별 형태소 빈도 집계.

언어별 분석:
  - 한국어: kiwipiepy 명사(NNG, NNP) 추출
  - 영어:   정규식 토크나이징 + 불용어 제거

Silver 리뷰가 쌓인 다음날 실행하도록 11:00 UTC 스케줄.
"""

from datetime import datetime

from airflow import DAG

from callbacks.slack_callback import slack_fail_alert
from operators.gold_reviews_to_morphemes import GoldReviewsToMorphemesOperator


with DAG(
    dag_id="steam_gold_01_review_morphemes",
    description="Silver 리뷰 parquet → gold_review_morphemes (워드클라우드용 형태소 빈도)",
    schedule="0 11 * * *",     # 매일 11:00 UTC (Silver 03 완료 후)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "gold"],
    default_args={"on_failure_callback": slack_fail_alert},
) as dag:

    build_morphemes = GoldReviewsToMorphemesOperator(
        task_id="build_morphemes",
        top_n=1000,             # 게임·언어별 상위 1000개 형태소 저장
        # app_id_limit=3,       # 테스트 시 소수 지정
    )
