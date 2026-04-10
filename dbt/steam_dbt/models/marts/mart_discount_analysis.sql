-- 할인 분석 마트
-- 게임별 할인 이력을 집계하여 할인 패턴 파악용 테이블 생성

SELECT
    p.app_id,
    g.name,
    COUNT(*)                                    AS discount_count,       -- 총 할인 감지 횟수
    ROUND(AVG(p.discount_percent))              AS avg_discount_pct,     -- 평균 할인율
    MAX(p.discount_percent)                     AS max_discount_pct,     -- 최대 할인율
    MIN(p.final_price)                          AS lowest_price,         -- 역대 최저가
    MAX(p.initial_price)                        AS original_price,       -- 원가
    MIN(p.collected_at)                         AS first_seen_discount,  -- 첫 할인 감지일
    MAX(p.collected_at)                         AS last_seen_discount     -- 최근 할인 감지일
FROM {{ ref('stg_price_history') }}  p
LEFT JOIN {{ ref('stg_games') }}     g ON p.app_id = g.app_id
WHERE p.discount_percent > 0
GROUP BY p.app_id, g.name
