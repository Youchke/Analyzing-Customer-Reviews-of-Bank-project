--Fichier : models/views/vw_customer_experience.sql

{{
  config(
    materialized='view',
    tags=['reporting']
  )
}}

SELECT
  -- Détection des tendances temporelles
  d.day,
  d.month,
  AVG(r.review_rating) AS avg_rating,
  
  -- Analyse des topics critiques
  STRING_AGG(DISTINCT 
    CASE WHEN s.sentiment = 'negative' THEN t.topic_name ELSE NULL END, 
    ', '
  ) AS critical_topics,
  STRING_AGG(DISTINCT 
    CASE WHEN s.sentiment = 'positive' THEN t.topic_name ELSE NULL END, 
    ', '
  ) AS positive_topics,
  
  -- Corrélations
  CORR(r.review_rating, s.sentiment_score) AS rating_sentiment_correlation
FROM {{ ref('fact_reviews') }} r
JOIN {{ ref('dim_date') }} d ON r.date_id = d.date_id
JOIN {{ ref('dim_topic') }} t ON r.topic_id = t.topic_id
JOIN {{ ref('dim_sentiment') }} s ON r.sentiment_id = s.sentiment_id
GROUP BY 1, 2