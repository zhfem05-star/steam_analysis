-- ============================================================
-- fact_price_history 테이블 컬럼 추가 마이그레이션
-- 이미 실행 중인 컨테이너에 수동으로 적용 필요:
--   docker exec -i analytics-postgres psql -U analytics -d steam_analytics < init-db/03_alter_fact_price_history.sql
-- ============================================================

ALTER TABLE fact_price_history
    ADD COLUMN IF NOT EXISTS discount_end_at TIMESTAMP;

DO $$
BEGIN
    RAISE NOTICE 'fact_price_history.discount_end_at 컬럼 추가 완료';
END $$;
