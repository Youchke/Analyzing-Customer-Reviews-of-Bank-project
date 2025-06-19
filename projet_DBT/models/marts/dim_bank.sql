{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['bank_name']) }} AS bank_id,
    bank_name
FROM {{ source('source_data', 'raw_reviews_brutes') }}
WHERE bank_name IS NOT NULL
