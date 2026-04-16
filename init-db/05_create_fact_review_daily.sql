-- fact_review_daily: 게임별·언어별·날짜별 리뷰 수 일별 집계 스냅샷 테이블
--
-- 용도:
--   - 할인 기간 vs 비할인 기간 리뷰 작성 수 A/B 비교
--   - fact_price_history와 review_date ↔ collected_at JOIN으로 할인 여부 판별
--
-- PRIMARY KEY (app_id, language, review_date): 같은 날짜로 재실행해도 UPSERT 보장

CREATE TABLE IF NOT EXISTS fact_review_daily (
    app_id          INTEGER      NOT NULL,
    language        VARCHAR(20)  NOT NULL,
    review_date     DATE         NOT NULL,
    review_count    INTEGER      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (app_id, language, review_date)
);

-- 할인-리뷰 JOIN 시 자주 쓰이는 패턴 최적화
CREATE INDEX IF NOT EXISTS idx_review_daily_app_date
    ON fact_review_daily (app_id, review_date);

CREATE INDEX IF NOT EXISTS idx_review_daily_date
    ON fact_review_daily (review_date);
