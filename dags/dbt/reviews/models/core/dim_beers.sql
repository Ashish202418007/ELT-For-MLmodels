SELECT DISTINCT
    beer_id,
    beer_name,
    brewery_id,
    beer_abv,
    beer_style
FROM {{ ref('stg_reviews') }}
