{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}

SELECT
    review_id,
    beer_id,
    reviewer_name AS reviewer_id,
    review_time,
    review_overall,
    review_aroma,
    review_appearance,
    review_palate,
    review_taste,
    review_text
FROM {{ ref('stg_reviews') }}

{% if is_incremental() %}
  -- only load reviews newer than the last one already in the table
  WHERE review_time > (SELECT MAX(review_time) FROM {{ this }})
{% endif %}
