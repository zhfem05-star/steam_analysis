-- 할인-리뷰 A/B 비교 마트
-- 할인 날짜를 기준으로 리뷰 작성 수 변화를 시각화하기 위한 테이블
--
-- 사용 예시:
--   - X축: 날짜, Y축: 리뷰 수, 할인 구간 하이라이트
--   - 할인 시작일 전후 N일 리뷰 수 추이 비교
--   - A그룹(할인 기간) vs B그룹(비할인 기간) 리뷰 수 평균 비교

WITH review_by_date AS (
    -- 언어 전체 합산 (언어 필터 없이 총 리뷰 수)
    SELECT
        app_id,
        review_date,
        SUM(review_count)                          AS total_review_count,
        SUM(CASE WHEN language = 'korean'  THEN review_count ELSE 0 END) AS korean_review_count,
        SUM(CASE WHEN language = 'english' THEN review_count ELSE 0 END) AS english_review_count
    FROM {{ ref('stg_review_daily') }}
    GROUP BY app_id, review_date
),

price_by_date AS (
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
    r.app_id,
    g.name                                      AS game_name,
    r.review_date                               AS date,

    -- 리뷰 수
    r.total_review_count,
    r.korean_review_count,
    r.english_review_count,

    -- 할인 정보 (당일 수집 기록 없으면 비할인으로 처리)
    COALESCE(p.is_discounted,    FALSE)         AS is_discounted,
    COALESCE(p.discount_percent, 0)             AS discount_percent,
    p.initial_price,
    p.final_price,
    p.discount_end_at,

    -- A/B 그룹 레이블 (시각화 필터용)
    CASE
        WHEN COALESCE(p.is_discounted, FALSE) THEN 'discount'
        ELSE 'normal'
    END                                         AS ab_group,

    -- 할인 전 7일 평균 리뷰 수 (비교 기준선)
    AVG(r.total_review_count) OVER (
        PARTITION BY r.app_id
        ORDER BY r.review_date
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    )                                           AS baseline_7d_avg

FROM review_by_date                                r
LEFT JOIN price_by_date                            p ON r.app_id     = p.app_id
                                                   AND r.review_date = p.price_date
LEFT JOIN {{ ref('stg_games') }}                   g ON r.app_id     = g.app_id
ORDER BY r.app_id, r.review_date
