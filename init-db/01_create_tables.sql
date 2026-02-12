-- ============================================================
-- Steam 분석 프로젝트 - 초기 테이블 생성
-- analytics-postgres 컨테이너 시작 시 자동 실행
-- ============================================================

-- ========================
-- Dimension Tables
-- ========================

CREATE TABLE IF NOT EXISTS dim_games (
    app_id              INTEGER PRIMARY KEY,
    name                VARCHAR(500) NOT NULL,
    type                VARCHAR(50),
    is_free             BOOLEAN DEFAULT FALSE,
    required_age        INTEGER DEFAULT 0,
    developers          VARCHAR(500),
    publishers          VARCHAR(500),
    release_date        DATE,
    platforms_windows   BOOLEAN DEFAULT FALSE,
    platforms_mac       BOOLEAN DEFAULT FALSE,
    platforms_linux     BOOLEAN DEFAULT FALSE,
    header_image_url    VARCHAR(1000),
    total_reviews       INTEGER,
    total_recommendations INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_genres (
    genre_id    INTEGER PRIMARY KEY,
    genre_name  VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_game_genres (
    app_id      INTEGER REFERENCES dim_games(app_id),
    genre_id    INTEGER REFERENCES dim_genres(genre_id),
    PRIMARY KEY (app_id, genre_id)
);

-- ========================
-- Fact Tables
-- ========================

CREATE TABLE IF NOT EXISTS fact_concurrent_players (
    id                  SERIAL PRIMARY KEY,
    app_id              INTEGER NOT NULL,
    rank                INTEGER,
    concurrent_in_game  INTEGER NOT NULL,
    peak_in_game        INTEGER,
    collected_at        TIMESTAMP NOT NULL,
    api_last_update     TIMESTAMP,
    UNIQUE (app_id, collected_at)
);

CREATE TABLE IF NOT EXISTS fact_price_history (
    id                  SERIAL PRIMARY KEY,
    app_id              INTEGER NOT NULL,
    currency            VARCHAR(10),
    initial_price       INTEGER,
    final_price         INTEGER,
    discount_percent    INTEGER DEFAULT 0,
    is_free             BOOLEAN DEFAULT FALSE,
    collected_at        DATE NOT NULL,
    UNIQUE (app_id, collected_at)
);

CREATE TABLE IF NOT EXISTS fact_reviews (
    recommendation_id       BIGINT PRIMARY KEY,
    app_id                  INTEGER NOT NULL,
    author_steamid          VARCHAR(50),
    language                VARCHAR(20),
    review_text             TEXT,
    voted_up                BOOLEAN,
    votes_up                INTEGER,
    votes_funny             INTEGER,
    weighted_vote_score     FLOAT,
    playtime_forever        INTEGER,
    playtime_at_review      INTEGER,
    playtime_last_two_weeks INTEGER,
    author_num_games_owned  INTEGER,
    author_num_reviews      INTEGER,
    steam_purchase          BOOLEAN,
    received_for_free       BOOLEAN,
    written_during_early_access BOOLEAN,
    timestamp_created       TIMESTAMP,
    timestamp_updated       TIMESTAMP,
    collected_at            DATE NOT NULL
);

-- ========================
-- 인덱스 (쿼리 성능용)
-- ========================

CREATE INDEX IF NOT EXISTS idx_concurrent_app_time
    ON fact_concurrent_players (app_id, collected_at);

CREATE INDEX IF NOT EXISTS idx_price_app_date
    ON fact_price_history (app_id, collected_at);

CREATE INDEX IF NOT EXISTS idx_reviews_app
    ON fact_reviews (app_id);

CREATE INDEX IF NOT EXISTS idx_reviews_language
    ON fact_reviews (language);

CREATE INDEX IF NOT EXISTS idx_game_genres_genre
    ON dim_game_genres (genre_id);

-- ========================
-- 완료 메시지
-- ========================
DO $$
BEGIN
    RAISE NOTICE 'Steam Analytics 테이블 생성 완료!';
END $$;
