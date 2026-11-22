-- ========================
-- 동접자 관리 목록·시계열 테이블 (재구성 4단계)
-- ========================
--
-- player_watchlist
--   전체 게임의 스캔 일정과 매시 수집 대상(is_active)을 함께 관리한다.
--   - 고정 대상: first_top100_at이 있거나 source = 'manual'. 예산 재배분으로 해제하지 않음
--   - 예산 대상: 고정 대상이 아닌 게임 중 recent_peak 상위 N개
--   - 활성 게임은 매시 수집으로 값을 얻으므로 일별 스캔에서 제외
--
-- fact_player_counts
--   매시 수집 결과 원본. 기간 제한 없이 보관하고 월별 파티션으로 나눈다.
--   파티션은 적재 오퍼레이터가 필요한 달을 생성한다.
--   기존 fact_concurrent_players(하루 2회, 구 구조)는 7단계 dbt 전환 후 제거한다.

CREATE TABLE IF NOT EXISTS player_watchlist (
    app_id                 INTEGER     PRIMARY KEY,
    source                 TEXT        NOT NULL CHECK (source IN ('scan', 'manual', 'top100')),
    first_top100_at        TIMESTAMPTZ,                         -- 값이 있으면 고정 수집 대상
    is_active              BOOLEAN     NOT NULL DEFAULT FALSE,  -- 매시 개별 수집 대상
    last_player_count      INTEGER,                             -- 가장 최근 관측값 (매시 수집 또는 스캔)
    last_observed_at       TIMESTAMPTZ,
    recent_peak            INTEGER,                             -- 예산 정렬 기준
    last_scanned_at        TIMESTAMPTZ,
    next_scan_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    scan_interval_days     INTEGER     NOT NULL DEFAULT 1 CHECK (scan_interval_days >= 1),
    consecutive_low_scans  INTEGER     NOT NULL DEFAULT 0,
    scan_priority          BOOLEAN     NOT NULL DEFAULT FALSE,  -- 다음 스캔에서 간격과 무관하게 우선
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 일별 스캔 대상 조회 (비활성 게임 중 일정 도래)
CREATE INDEX IF NOT EXISTS idx_player_watchlist_scan_due
    ON player_watchlist (next_scan_at) WHERE NOT is_active;

CREATE INDEX IF NOT EXISTS idx_player_watchlist_active
    ON player_watchlist (app_id) WHERE is_active;

-- 매시 동접자 (collected_at = 수집 슬롯 정시, fetched_at = 실제 호출 시각)
CREATE TABLE IF NOT EXISTS fact_player_counts (
    app_id         INTEGER     NOT NULL,
    collected_at   TIMESTAMPTZ NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL,
    player_count   INTEGER,                    -- 수집 실패·데이터 없음은 NULL
    status         TEXT        NOT NULL CHECK (status IN ('ok', 'not_found', 'error')),
    source         TEXT        NOT NULL CHECK (source IN ('top100', 'individual')),
    rank           SMALLINT,                   -- top100 응답 순위
    peak_in_game   INTEGER,                    -- top100 응답의 24시간 최고값
    PRIMARY KEY (app_id, collected_at)
) PARTITION BY RANGE (collected_at);
