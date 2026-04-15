-- 할인-동접자 상관관계 마트
-- 할인 날짜를 기준으로 동접자 수 변화를 시각화하기 위한 테이블
--
-- 사용 예시:
--   - X축: 날짜, Y축: 동접자 수, 할인 구간 하이라이트
--   - 할인 시작일 전후 N일 동접자 추이 비교
--   - 할인율별 동접자 증감 패턴 분석

WITH daily_players AS (
    -- 동접자 일평균 (하루 2회 스냅샷 집계)
    SELECT
        app_id,
        snapshot_date,
        ROUND(AVG(concurrent_in_game)) AS avg_concurrent,
        MAX(concurrent_in_game)        AS max_concurrent
    FROM {{ ref('stg_concurrent_players') }}
    GROUP BY app_id, snapshot_date
),

price_by_date AS (
    -- 날짜별 할인 정보 (당일 수집된 가격 기준)
    SELECT
        app_id,
        collected_at          AS price_date,
        discount_percent,
        initial_price,
        final_price,
        discount_end_at,
        discount_percent > 0  AS is_discounted
    FROM {{ ref('stg_price_history') }}
)

SELECT
    p.app_id,
    g.name                              AS game_name,
    COALESCE(pr.price_date, p.snapshot_date) AS date,

    -- 동접자
    p.avg_concurrent,
    p.max_concurrent,

    -- 할인 정보 (당일 할인 없으면 0 / NULL)
    COALESCE(pr.discount_percent, 0)    AS discount_percent,
    COALESCE(pr.is_discounted, FALSE)   AS is_discounted,
    pr.initial_price,
    pr.final_price,
    pr.discount_end_at,

    -- 할인 전 7일 평균 동접자 (비교 기준선)
    AVG(p.avg_concurrent) OVER (
        PARTITION BY p.app_id
        ORDER BY p.snapshot_date
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    )                                   AS baseline_7d_avg

FROM daily_players                         p
LEFT JOIN price_by_date                    pr ON p.app_id = pr.app_id
                                           AND p.snapshot_date = pr.price_date
LEFT JOIN {{ ref('stg_games') }}           g  ON p.app_id = g.app_id
ORDER BY p.app_id, date
