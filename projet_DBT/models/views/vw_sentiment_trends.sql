--Fichier : models/views/vw_sentiment_trends.sql

{{
  config(
    materialized='view',
    tags=['reporting']
  )
}}

SELECT
  b.bank_name,
  br.branch_name,
  d.full_date As review_date ,
  d.month AS review_month,
  d.year AS review_year,
  s.sentiment,
  COUNT(*) AS review_count,
  AVG(s.sentiment_score) AS avg_sentiment_score,
  AVG(CASE WHEN s.sentiment = 'positive' THEN 1 ELSE 0 END) * 100 AS positive_percentage
FROM {{ ref('fact_reviews') }} r
JOIN {{ ref('dim_bank') }} b ON r.bank_id = b.bank_id
JOIN {{ ref('dim_branch') }} br ON r.branch_id = br.branch_id
JOIN {{ ref('dim_sentiment') }} s ON r.sentiment_id = s.sentiment_id
JOIN {{ ref('dim_date') }} d ON r.date_id = d.date_id
GROUP BY 1, 2, 3, 4, 5, 6
