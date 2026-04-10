-- Silver fact_concurrent_players 참조

SELECT
    app_id,
    concurrent_in_game,
    collected_at::DATE   AS snapshot_date,
    collected_at         AS snapshot_at
FROM {{ source('steam_silver', 'fact_concurrent_players') }}
