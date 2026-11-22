-- ========================
-- 스토어 카탈로그·가격 구간 테이블 (재구성 3단계)
-- ========================
--
-- 매일 전체 스냅샷을 비교해 값이 바뀐 시점만 구간으로 저장한다.
-- 일별 전체 스냅샷 원본은 S3 parquet(steam-raw)에 보관한다.
--
-- 구간 의미
--   valid_from   : 이 값이 처음 관측된 스냅샷 시각
--   valid_to     : 다른 값 또는 미관측이 처음 확인된 스냅샷 시각 (NULL = 현재 유효)
--   last_seen_at : 이 값이 마지막으로 관측된 스냅샷 시각
-- 실제 변경 시각은 (직전 구간 last_seen_at, valid_from] 사이 어딘가다.
-- last_seen_at이 valid_to보다 크게 앞서면 그 사이 수집 누락이 있었다는 뜻이다.

-- GetAppList 기준 게임 목록 (DLC·소프트웨어·영상·하드웨어 제외)
CREATE TABLE IF NOT EXISTS dim_apps (
    app_id               INTEGER     PRIMARY KEY,
    name                 TEXT,
    last_modified        TIMESTAMPTZ,
    price_change_number  BIGINT,
    is_listed            BOOLEAN     NOT NULL DEFAULT TRUE,  -- 최신 완료 스냅샷에 존재 여부
    first_seen_at        TIMESTAMPTZ NOT NULL,
    last_seen_at         TIMESTAMPTZ NOT NULL,
    delisted_at          TIMESTAMPTZ                          -- 목록에서 처음 사라진 스냅샷 시각
);

-- KR 스토어 가격 구간 (cents 단위는 Steam 응답 값 그대로)
CREATE TABLE IF NOT EXISTS fact_price_intervals (
    app_id               INTEGER     NOT NULL,
    valid_from           TIMESTAMPTZ NOT NULL,
    valid_to             TIMESTAMPTZ,
    last_seen_at         TIMESTAMPTZ NOT NULL,
    country_code         CHAR(2)     NOT NULL DEFAULT 'KR',
    is_free              BOOLEAN     NOT NULL,
    has_price            BOOLEAN     NOT NULL,                -- FALSE = 무료이거나 KR 구매 불가
    original_price_cents BIGINT,
    final_price_cents    BIGINT,
    discount_pct         SMALLINT    NOT NULL DEFAULT 0,
    discount_end_at      TIMESTAMPTZ,
    PRIMARY KEY (app_id, valid_from)
);

-- 게임별 열린 구간은 최대 1개
CREATE UNIQUE INDEX IF NOT EXISTS uq_price_intervals_open
    ON fact_price_intervals (app_id) WHERE valid_to IS NULL;

-- KR 스토어 리뷰 요약 구간 (Query reviews.summary_filtered)
CREATE TABLE IF NOT EXISTS fact_review_summary_intervals (
    app_id               INTEGER     NOT NULL,
    valid_from           TIMESTAMPTZ NOT NULL,
    valid_to             TIMESTAMPTZ,
    last_seen_at         TIMESTAMPTZ NOT NULL,
    review_count         INTEGER,
    percent_positive     SMALLINT,
    review_score         SMALLINT,
    PRIMARY KEY (app_id, valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_review_summary_intervals_open
    ON fact_review_summary_intervals (app_id) WHERE valid_to IS NULL;

-- 적용한 스냅샷 기록 (같은 스냅샷 재적용 방지·순서 역전 차단)
CREATE TABLE IF NOT EXISTS silver_snapshot_log (
    source               TEXT        NOT NULL,
    snapshot_id          TEXT        NOT NULL,
    collected_at         TIMESTAMPTZ NOT NULL,
    records              INTEGER     NOT NULL,
    applied_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, snapshot_id)
);
