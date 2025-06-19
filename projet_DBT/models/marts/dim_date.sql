{{ config(materialized='table') }}

SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['review_date']) }} AS date_id,
    review_date::TEXT::DATE AS full_date,
    EXTRACT(DAY FROM review_date::TEXT::DATE) AS day,
    EXTRACT(MONTH FROM review_date::TEXT::DATE) AS month,
    EXTRACT(YEAR FROM review_date::TEXT::DATE) AS year
FROM {{ source('source_data', 'raw_reviews_brutes') }}
WHERE review_date IS NOT NULL