-- models/staging/stg_reviews_cleaned.sql
/*
**Clean the Data (DBT & SQL)**
* Remove duplicate reviews.
* Normalize text (lowercase, remove punctuation, stop words).
* Handle missing values.

Modèle de nettoyage complet des avis bancaires avec améliorations:
- Suppression des doublons basée sur plusieurs critères
- Normalisation avancée du texte (casse, ponctuation, mots vides)
- Gestion intelligente des valeurs manquantes avec fallback
- Validation et nettoyage des données de rating
- Support multilingue (Français/Arabe/Anglais)
- Correction PostgreSQL pour REGEXP_REPLACE
- Optimisations de performance
*/

{{ config(
    materialized='table',
    tags=['cleaning', 'reviews', 'final', 'data_quality'],
    indexes=[
        {'columns': ['bank_name'], 'type': 'btree'},
        {'columns': ['review_date'], 'type': 'btree'},
        {'columns': ['rating_source'], 'type': 'btree'}
    ]
) }}

WITH raw_data AS (
    SELECT * FROM {{ source('source_data', 'staging_reviews') }}
    WHERE review_text IS NOT NULL 
      AND TRIM(review_text) != ''  
      AND LENGTH(TRIM(review_text)) >= 3  -- Filtrer dès le début pour plus d'efficacité
),

-- Étape 1: Suppression des doublons avec critères améliorés
deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                LOWER(TRIM(bank_name)), 
                LOWER(TRIM(branch_name)), 
                LOWER(TRIM(review_text)),
                review_date
            ORDER BY 
                CASE WHEN review_rating IS NOT NULL THEN 1 ELSE 2 END,
                CASE WHEN bank_rating IS NOT NULL THEN 1 ELSE 2 END,
                review_date DESC
        ) AS duplicate_rank
    FROM raw_data
),

clean_duplicates AS (
    SELECT
        bank_name,     
        branch_name,  
        bank_rating,  
        bank_location,
        review_date,  
        review_text,  
        review_rating
    FROM deduplicated
    WHERE duplicate_rank = 1
),

-- Étape 2: Normalisation avancée du texte et nettoyage des données
normalized_text AS (
    SELECT
        TRIM(bank_name) AS bank_name,     
        TRIM(branch_name) AS branch_name,  
        
        -- Conversion du bank_rating (format "1,6" vers décimal)
        CASE 
            WHEN bank_rating IS NOT NULL THEN 
                CAST(REPLACE(CAST(bank_rating AS VARCHAR), ',', '.') AS DECIMAL(3,2))
            ELSE NULL 
        END AS bank_rating_decimal,
        
        TRIM(bank_location) AS bank_location,    
        
        -- Normalisation du texte (version de base compatible PostgreSQL)
        {{ clean_text_basic('review_text', false) }} AS cleaned_text,
        
        -- Version avec stop words (optionnelle, plus lente)
        {{ normalize_text('review_text', {
            'remove_stopwords': true,
            'min_length': 3,
            'keep_numbers': true,
            'languages': ['fr']
        }) }} AS cleaned_text_advanced,
        
        -- Validation du review_rating
        CASE 
            WHEN review_rating BETWEEN 1 AND 5 THEN review_rating
            WHEN review_rating > 5 THEN 5
            WHEN review_rating < 1 THEN 1
            ELSE NULL 
        END AS review_rating_clean,
        
        review_date,
        review_text -- Garder le texte original pour les métriques
        
    FROM clean_duplicates
    WHERE {{ is_valid_text('review_text', 3) }}
),

-- Étape 3: Calcul des métriques de qualité après normalisation
text_with_metrics AS (
    SELECT 
        *,
        -- Métriques de qualité du texte calculées directement
        LENGTH(review_text) as original_length,
        LENGTH(cleaned_text) as normalized_length,
        CASE 
            WHEN LENGTH(review_text) > 0 THEN
                ROUND(
                    LENGTH(cleaned_text) * 100.0 / LENGTH(review_text), 
                    2
                )
            ELSE 0
        END as retention_percentage,
        CASE 
            WHEN cleaned_text IS NOT NULL THEN
                ARRAY_LENGTH(
                    STRING_TO_ARRAY(TRIM(cleaned_text), ' '), 
                    1
                )
            ELSE 0
        END as word_count
    FROM normalized_text
),

-- Étape 4: Sélection du meilleur texte nettoyé selon les critères
text_selection AS (
    SELECT 
        *,
        -- Choisir le meilleur texte selon les critères de qualité
        CASE 
            WHEN retention_percentage >= 60 AND word_count >= 3 THEN cleaned_text_advanced
            WHEN cleaned_text IS NOT NULL AND LENGTH(cleaned_text) >= 5 THEN cleaned_text
            ELSE cleaned_text
        END AS final_cleaned_text
    FROM text_with_metrics
    WHERE cleaned_text IS NOT NULL 
      AND LENGTH(cleaned_text) >= 3
),

-- Étape 5: Calcul des statistiques par banque pour gestion des valeurs manquantes
bank_statistics AS (
    SELECT 
        bank_name,
        AVG(CAST(review_rating_clean AS DECIMAL(3,1))) AS avg_rating,
        COUNT(review_rating_clean) AS rating_count,
        MIN(review_rating_clean) AS min_rating,
        MAX(review_rating_clean) AS max_rating,
        STDDEV(CAST(review_rating_clean AS DECIMAL(3,1))) AS rating_stddev,
        AVG(bank_rating_decimal) AS avg_bank_rating,
        AVG(normalized_length) AS avg_text_length,
        AVG(retention_percentage) AS avg_retention
    FROM text_selection 
    WHERE review_rating_clean IS NOT NULL
    GROUP BY bank_name
),

-- Calcul de la moyenne globale comme fallback
global_stats AS (
    SELECT 
        AVG(CAST(review_rating_clean AS DECIMAL(3,1))) AS global_avg_rating,
        AVG(bank_rating_decimal) AS global_avg_bank_rating,
        AVG(normalized_length) AS global_avg_text_length
    FROM text_selection 
    WHERE review_rating_clean IS NOT NULL
)

-- Résultat final avec gestion avancée des valeurs manquantes et contrôles qualité
SELECT
    -- Génération d'un ID unique basé sur les données
    MD5(CONCAT(
        COALESCE(n.bank_name, ''),
        COALESCE(n.branch_name, ''),
        COALESCE(n.review_date::TEXT, ''),
        COALESCE(n.final_cleaned_text, '')
    )) AS id,
    
    n.bank_name,     
    n.branch_name,  
    
    -- Bank rating normalisé
    CASE 
        WHEN n.bank_rating_decimal BETWEEN 0 AND 5 THEN n.bank_rating_decimal
        WHEN s.avg_bank_rating IS NOT NULL THEN ROUND(s.avg_bank_rating, 1)
        WHEN g.global_avg_bank_rating IS NOT NULL THEN ROUND(g.global_avg_bank_rating, 1)
        ELSE 3.0
    END AS bank_rating,
    
    n.bank_location,
    n.review_date,
    
    -- Texte nettoyé final
    n.final_cleaned_text AS final_text,
    
    -- Gestion intelligente des ratings manquants
    CASE
        WHEN n.review_rating_clean IS NOT NULL THEN n.review_rating_clean
        WHEN s.avg_rating IS NOT NULL AND s.rating_count >= 3 THEN ROUND(s.avg_rating, 1)
        WHEN g.global_avg_rating IS NOT NULL THEN ROUND(g.global_avg_rating, 1)
        ELSE 3
    END AS review_rating,
    
    -- Indicateurs de qualité des données
    CASE 
        WHEN n.review_rating_clean IS NOT NULL THEN 'ORIGINAL'
        WHEN s.avg_rating IS NOT NULL AND s.rating_count >= 3 THEN 'BANK_AVERAGE'
        WHEN g.global_avg_rating IS NOT NULL THEN 'GLOBAL_AVERAGE'
        ELSE 'DEFAULT'
    END AS rating_source,
    
    -- Métriques de qualité des données étendues
    -- n.original_length,
    -- n.normalized_length,
    -- n.word_count,
    s.rating_count AS bank_rating_count,
    ROUND(s.avg_rating, 2) AS bank_avg_rating,
    
    -- Indicateurs de qualité du texte
    CASE 
        WHEN n.retention_percentage >= 70 THEN 'HIGH'
        WHEN n.retention_percentage >= 50 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS text_quality,
    
    -- Timestamp de traitement
    CURRENT_TIMESTAMP AS processed_at

FROM text_selection n
LEFT JOIN bank_statistics s ON n.bank_name = s.bank_name
CROSS JOIN global_stats g

-- Filtres de qualité finale stricts
WHERE n.bank_name IS NOT NULL 
  AND n.bank_name != ''
  AND n.review_date IS NOT NULL
  AND n.final_cleaned_text IS NOT NULL  
  AND TRIM(n.final_cleaned_text) != ''  
  AND LENGTH(TRIM(n.final_cleaned_text)) >= 3

-- Tri par qualité et date
ORDER BY 
    CASE WHEN n.review_rating_clean IS NOT NULL THEN 1 ELSE 2 END,
    n.retention_percentage DESC,
    n.review_date DESC,
    n.bank_name