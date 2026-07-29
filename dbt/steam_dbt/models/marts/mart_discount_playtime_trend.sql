-- 할인 구간별 플레이타임 A/B 비교 마트
-- 할인 기간 vs 비할인 기간의 리뷰 작성자 평균 플레이타임 비교

WITH playtime_by_date AS (
    SELECT
        app_id,
        review_date,
        SUM(review_count)                                                          AS total_review_count,
        SUM(review_count * avg_playtime_at_review) / NULLIF(SUM(review_count), 0) AS avg_playtime_at_review,
        SUM(review_count * avg_playtime_forever)   / NULLIF(SUM(review_count), 0) AS avg_playtime_forever
    FROM {{ ref('stg_review_playtime_daily') }}
    GROUP BY app_id, review_date
),

price_by_date AS (
    SELECT
        app_id,
        collected_at      AS price_date,
        discount_percent,
        discount_percent > 0 AS is_discounted
    FROM {{ ref('stg_price_history') }}
)

SELECT
    pt.app_id,
    g.name                                      AS game_name,
    pt.review_date                              AS date,

    ROUND(pt.avg_playtime_at_review::NUMERIC, 2) AS avg_playtime_at_review_min,
    ROUND(pt.avg_playtime_forever::NUMERIC,   2) AS avg_playtime_forever_min,
    ROUND(pt.avg_playtime_at_review::NUMERIC / 60, 2) AS avg_playtime_at_review_hr,
    ROUND(pt.avg_playtime_forever::NUMERIC   / 60, 2) AS avg_playtime_forever_hr,

    pt.total_review_count,

    COALESCE(p.is_discounted,    FALSE)         AS is_discounted,
    COALESCE(p.discount_percent, 0)             AS discount_percent,

    CASE
        WHEN COALESCE(p.is_discounted, FALSE) THEN 'discount'
        ELSE 'normal'
    END                                         AS ab_group,

    -- 할인 전 7일 평균 플레이타임 (비교 기준선)
    AVG(pt.avg_playtime_at_review) OVER (
        PARTITION BY pt.app_id
        ORDER BY pt.review_date
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    )                                           AS baseline_7d_avg_playtime

FROM playtime_by_date                              pt
LEFT JOIN price_by_date                            p  ON pt.app_id     = p.app_id
                                                    AND pt.review_date = p.price_date
LEFT JOIN {{ ref('stg_games') }}                   g  ON pt.app_id     = g.app_id
ORDER BY pt.app_id, pt.review_date
