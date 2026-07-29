SELECT
    app_id,
    language,
    review_date,
    avg_playtime_at_review,
    avg_playtime_forever,
    review_count,
    updated_at
FROM {{ source('steam_silver', 'fact_review_playtime_daily') }}
