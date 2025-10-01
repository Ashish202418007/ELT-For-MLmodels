{{ config(
    materialized='incremental',
    unique_key='beer_style||CAST(review_month AS STRING)'
) }}

SELECT
    b.beer_style,
    DATE_TRUNC(fr.review_time, MONTH) AS review_month,
    AVG(fr.review_overall) AS avg_rating,
    COUNT(fr.review_id) AS num_reviews
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_beers') }} b USING (beer_id)
GROUP BY b.beer_style, review_month
