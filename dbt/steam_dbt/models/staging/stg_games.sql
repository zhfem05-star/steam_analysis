-- Silver dim_games 그대로 참조하는 staging 뷰

SELECT
    app_id,
    name,
    type,
    is_free,
    required_age,
    developers,
    publishers,
    release_date,
    platforms_windows,
    platforms_mac,
    platforms_linux,
    header_image_url,
    total_recommendations,
    created_at,
    updated_at
FROM {{ source('steam_silver', 'dim_games') }}
