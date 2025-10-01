SELECT DISTINCT
    reviewer_name AS reviewer_id,
    reviewer_name
FROM {{ ref('stg_reviews') }}
