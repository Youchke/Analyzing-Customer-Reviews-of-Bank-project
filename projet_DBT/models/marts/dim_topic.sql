{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['topic_name']) }} AS topic_id,
    topic_name,
    topic_confidence
FROM {{ source('source_data', 'raw_reviews_brutes') }}
WHERE topic_name IS NOT NULL
