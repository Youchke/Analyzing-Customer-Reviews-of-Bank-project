--Fichier : models/views/vw_topic_analysis.sql

{{
  config(
    materialized='view',
    tags=['reporting']
  )
}}

WITH topic_stats AS (
  SELECT
    t.topic_name,
    s.sentiment,
    COUNT(*) AS occurrence_count,
    AVG(s.sentiment_score) AS avg_sentiment,
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY t.topic_name) * 100 AS sentiment_distribution
  FROM {{ ref('fact_reviews') }} r
  JOIN {{ ref('dim_topic') }} t ON r.topic_id = t.topic_id
  JOIN {{ ref('dim_sentiment') }} s ON r.sentiment_id = s.sentiment_id
  GROUP BY 1, 2
)

SELECT
  topic_name,
  sentiment,
  occurrence_count,
  avg_sentiment,
  sentiment_distribution,
  CASE 
    WHEN sentiment = 'positive' THEN RANK() OVER (ORDER BY occurrence_count DESC)
    ELSE NULL
  END AS positive_rank,
  CASE 
    WHEN sentiment = 'negative' THEN RANK() OVER (ORDER BY occurrence_count DESC)
    ELSE NULL
  END AS negative_rank
FROM topic_stats