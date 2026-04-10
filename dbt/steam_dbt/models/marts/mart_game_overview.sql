-- 게임 종합 정보 마트
-- dim_games + 장르 목록 + 최신 가격 정보를 하나의 테이블로 결합

WITH latest_price AS (
    -- 게임별 가장 최근 가격 스냅샷 1건
    SELECT DISTINCT ON (app_id)
        app_id,
        initial_price,
        final_price,
        discount_percent,
        is_free,
        collected_at AS price_collected_at
    FROM {{ ref('stg_price_history') }}
    ORDER BY app_id, collected_at DESC
),

genre_agg AS (
    -- 게임별 장르를 쉼표 구분 문자열로 집계
    SELECT
        dgg.app_id,
        STRING_AGG(dg.genre_name, ', ' ORDER BY dg.genre_name) AS genres
    FROM {{ source('steam_silver', 'dim_game_genres') }} dgg
    JOIN {{ source('steam_silver', 'dim_genres') }}      dg  ON dgg.genre_id = dg.genre_id
    GROUP BY dgg.app_id
)

SELECT
    g.app_id,
    g.name,
    g.type,
    g.is_free,
    g.developers,
    g.publishers,
    g.release_date,
    g.platforms_windows,
    g.platforms_mac,
    g.platforms_linux,
    g.total_recommendations,
    ga.genres,
    lp.initial_price,
    lp.final_price,
    lp.discount_percent,
    lp.price_collected_at
FROM {{ ref('stg_games') }}    g
LEFT JOIN genre_agg            ga ON g.app_id = ga.app_id
LEFT JOIN latest_price         lp ON g.app_id = lp.app_id
