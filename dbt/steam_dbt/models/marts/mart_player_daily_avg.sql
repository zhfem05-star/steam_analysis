-- 동접자 일평균 마트
-- 하루 2회 스냅샷(08:00 / 14:00 UTC)을 집계하여 일평균 동접자 수 산출

SELECT
    app_id,
    snapshot_date,
    ROUND(AVG(concurrent_in_game))  AS avg_concurrent,
    MAX(concurrent_in_game)         AS max_concurrent,
    MIN(concurrent_in_game)         AS min_concurrent,
    COUNT(*)                        AS snapshot_count   -- 당일 수집된 스냅샷 수
FROM {{ ref('stg_concurrent_players') }}
GROUP BY app_id, snapshot_date
