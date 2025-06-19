{{ config(materialized='table') }}

WITH branch_data AS (
    SELECT DISTINCT
        bank_name,
        branch_name
    FROM {{ source('source_data', 'raw_reviews_brutes') }}
    WHERE branch_name IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['br.branch_name']) }} AS branch_id,
    b.bank_id,
    br.branch_name
FROM branch_data br
JOIN {{ ref('dim_bank') }} b
  ON br.bank_name = b.bank_name
