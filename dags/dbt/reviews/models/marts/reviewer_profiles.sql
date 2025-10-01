{{ config(
    materialized='incremental',
    unique_key='reviewer_id'
) }}

SELECT
    r.reviewer_id,
    COUNT(fr.review_id) AS total_reviews,
    AVG(fr.review_overall) AS avg_score_given,
    MAX(fr.review_time) AS last_review_date
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_reviewers') }} r ON fr.reviewer_id = r.reviewer_id
GROUP BY r.reviewer_id
