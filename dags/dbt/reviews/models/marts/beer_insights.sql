{{ config(
    materialized='incremental',
    unique_key='beer_id'
) }}

SELECT
    b.beer_id,
    b.beer_name,
    b.beer_style,
    AVG(fr.review_overall) AS avg_rating,
    COUNT(fr.review_id) AS num_reviews
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_beers') }} b USING (beer_id)
GROUP BY b.beer_id, b.beer_name, b.beer_style
