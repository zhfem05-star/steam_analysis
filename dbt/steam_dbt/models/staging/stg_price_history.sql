-- Silver fact_price_history 참조

SELECT
    app_id,
    currency,
    initial_price,
    final_price,
    discount_percent,
    is_free,
    discount_end_at,
    collected_at
FROM {{ source('steam_silver', 'fact_price_history') }}
