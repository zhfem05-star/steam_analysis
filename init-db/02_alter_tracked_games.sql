-- ============================================================
-- tracked_games 컨트롤 테이블 컬럼 추가 마이그레이션
-- 기존 컨테이너(데이터가 이미 있는 경우)에 수동으로 실행
-- docker exec -i analytics-postgres psql -U analytics -d steam_analytics < init-db/02_alter_tracked_games.sql
-- ============================================================

-- ============================================================
-- tracked_games 컨트롤 테이블 역할
-- ============================================================
-- 각 DAG가 "어떤 게임을", "어떤 데이터를" 수집할지 DB에서 제어하기 위한 설정 테이블.
-- 코드 수정 없이 UPDATE 한 줄로 수집 대상과 범위를 조정할 수 있다.
--
-- [기존 컬럼]
--   appid           : Steam 게임 고유 ID (PK)
--   first_seen      : 이 게임이 tracked_games에 처음 등록된 날짜
--   last_discounted : 마지막으로 할인이 감지된 날짜
--
-- [추가 컬럼 설명]
--   is_active
--     - TRUE  : 02(리뷰), 03(동접자) DAG 모두 이 게임을 수집 대상으로 포함
--     - FALSE : 모든 수집에서 제외 (서비스 종료 게임, 테스트 제외 등에 사용)
--
--   collect_reviews
--     - TRUE  : 02 리뷰 DAG가 이 게임의 리뷰를 수집
--     - FALSE : 리뷰 수집만 건너뜀 (리뷰가 너무 많거나 불필요한 게임에 사용)
--
--   collect_players
--     - TRUE  : 03 동접자 DAG가 이 게임의 동접자 수를 수집
--     - FALSE : 동접자 수집만 건너뜀
--
--   reviews_collected_at
--     - 이 게임의 리뷰를 마지막으로 성공적으로 수집 완료한 시각 (UTC)
--     - NULL이면 아직 한 번도 수집하지 않은 게임
--     - 수집 이력 확인 및 오래된 게임 재수집 판단에 활용
--
--   review_cursors
--     - Steam 리뷰 API의 언어별 페이지네이션 커서를 저장
--     - 예: {"korean": "AoJ_xxx", "english": "AoJ_yyy"}
--     - 수집 중단 후 재실행 시 처음부터 다시 받지 않고 이 커서부터 이어서 수집
--     - {} (빈 객체)이면 다음 수집 시 처음("*")부터 전체 수집
-- ============================================================

ALTER TABLE tracked_games
    ADD COLUMN IF NOT EXISTS is_active            BOOLEAN   NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS collect_reviews      BOOLEAN   NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS collect_players      BOOLEAN   NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS reviews_collected_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS review_cursors       JSONB     NOT NULL DEFAULT '{}';

DO $$
BEGIN
    RAISE NOTICE 'tracked_games 마이그레이션 완료';
END $$;
