SELECT DISTINCT
    brewery_id,
    ANY_VALUE(beer_name) AS sample_beer
FROM {{ ref('stg_reviews') }}
