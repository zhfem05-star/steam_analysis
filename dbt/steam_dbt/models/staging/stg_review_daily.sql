-- Silver fact_review_daily 참조

SELECT
    app_id,
    language,
    review_date,
    review_count
FROM {{ source('steam_silver', 'fact_review_daily') }}
