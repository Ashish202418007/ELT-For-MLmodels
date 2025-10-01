-- staging model: clean the raw data

SELECT
    CAST(review_id AS STRING) AS review_id,
    CAST(beer_id AS STRING) AS beer_id,
    TRIM(beer_name) AS beer_name,
    CAST(brewery_id AS STRING) AS brewery_id,
    CAST(beer_abv AS FLOAT64) AS beer_abv,
    TRIM(beer_style) AS beer_style,
    CAST(review_overall AS FLOAT64) AS review_overall,
    CAST(review_aroma AS FLOAT64) AS review_aroma,
    CAST(review_appearance AS FLOAT64) AS review_appearance,
    CAST(review_palate AS FLOAT64) AS review_palate,
    CAST(review_taste AS FLOAT64) AS review_taste,
    CAST(review_time AS TIMESTAMP) AS review_time,
    TRIM(reviewer_name) AS reviewer_name,
    review_text
FROM {{ source('raw_dataset', 'daily_beer_reviews') }}
