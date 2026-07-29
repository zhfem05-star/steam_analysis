CREATE TABLE IF NOT EXISTS fact_review_playtime_daily (
    app_id                 INTEGER        NOT NULL,
    language               VARCHAR(20)    NOT NULL,
    review_date            DATE           NOT NULL,
    avg_playtime_at_review NUMERIC(10, 2),   -- 리뷰 작성 시점 평균 플레이타임 (분)
    avg_playtime_forever   NUMERIC(10, 2),   -- 전체 평균 플레이타임 (분)
    review_count           INTEGER        NOT NULL DEFAULT 0,
    updated_at             TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (app_id, language, review_date)
);
