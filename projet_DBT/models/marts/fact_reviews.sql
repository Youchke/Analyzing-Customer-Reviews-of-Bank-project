WITH review_data AS (
    SELECT
        r.id AS review_id,
        r.bank_name,
        r.branch_name,
        r.bank_location,
        b.city,
        b.region,
        r.sentiment,
        r.sentiment_score,
        r.topic_name,
        r.review_date,
        r.final_text,
        r.language,
        r.review_rating
    FROM {{ source('source_data', 'raw_reviews_brutes') }} r
    JOIN {{ ref('dim_location') }} b
        ON r.bank_location = b.bank_location  -- Joins les deux tables par branch_name
)

-- Création de la table de faits avec les clés et mesures
SELECT
    review_id,
    {{ dbt_utils.generate_surrogate_key(['bank_name']) }} AS bank_id,
    {{ dbt_utils.generate_surrogate_key(['branch_name']) }} AS branch_id,
    {{ dbt_utils.generate_surrogate_key(['city', 'region']) }} AS location_id,
    {{ dbt_utils.generate_surrogate_key(['sentiment', 'sentiment_score']) }} AS sentiment_id,
    {{ dbt_utils.generate_surrogate_key(['topic_name']) }} AS topic_id,
    {{ dbt_utils.generate_surrogate_key(['review_date']) }} AS date_id,
    final_text,
    language,
    review_rating
FROM review_data
