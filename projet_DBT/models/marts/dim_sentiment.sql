{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['sentiment', 'sentiment_score']) }} AS sentiment_id,
    sentiment,
    sentiment_score
FROM {{ source('source_data', 'raw_reviews_brutes') }}
WHERE sentiment IS NOT NULL
