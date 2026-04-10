-- ============================================================
-- gold_review_morphemes 테이블 생성 마이그레이션
-- 이미 실행 중인 컨테이너에 수동으로 적용 필요:
--   docker exec -i analytics-postgres psql -U analytics -d steam_analytics < init-db/04_create_gold_morphemes.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_review_morphemes (
    app_id              INTEGER      NOT NULL,
    language            VARCHAR(20)  NOT NULL,
    morpheme            VARCHAR(200) NOT NULL,
    frequency           INTEGER      NOT NULL,
    review_appearances  INTEGER      NOT NULL,
    earliest_review_at  TIMESTAMP,
    latest_review_at    TIMESTAMP,
    updated_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (app_id, language, morpheme)
);

CREATE INDEX IF NOT EXISTS idx_morphemes_app_lang
    ON gold_review_morphemes (app_id, language);

CREATE INDEX IF NOT EXISTS idx_morphemes_frequency
    ON gold_review_morphemes (app_id, language, frequency DESC);

DO $$
BEGIN
    RAISE NOTICE 'gold_review_morphemes 테이블 및 인덱스 생성 완료';
END $$;
