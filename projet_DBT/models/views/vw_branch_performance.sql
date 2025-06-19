--Fichier : models/views/vw_branch_performance.sql

{{
  config(
    materialized='view',
    tags=['reporting']
  )
}}

WITH branch_metrics AS (
  SELECT
    b.bank_name,
    br.branch_name,
    l.city,
    l.region,
    COUNT(*) AS total_reviews,
    AVG(r.review_rating) AS avg_rating,
    AVG(s.sentiment_score) AS avg_sentiment,
    AVG(CASE WHEN s.sentiment = 'positive' THEN 1 ELSE 0 END) * 100 AS positive_percentage,
    AVG(CASE WHEN r.review_rating >= 4 THEN 1 ELSE 0 END) * 100 AS promoter_percentage
  FROM {{ ref('fact_reviews') }} r
  JOIN {{ ref('dim_bank') }} b ON r.bank_id = b.bank_id
  JOIN {{ ref('dim_branch') }} br ON r.branch_id = br.branch_id
  JOIN {{ ref('dim_location') }} l ON r.location_id = l.location_id
  JOIN {{ ref('dim_sentiment') }} s ON r.sentiment_id = s.sentiment_id
  GROUP BY 1, 2, 3, 4
)

SELECT
  *,
  (avg_rating * 0.6 + positive_percentage * 0.4) AS performance_score,
  RANK() OVER (ORDER BY (avg_rating * 0.6 + positive_percentage * 0.4) DESC) AS overall_rank,
  RANK() OVER (PARTITION BY region ORDER BY avg_rating DESC) AS regional_rank
FROM branch_metrics