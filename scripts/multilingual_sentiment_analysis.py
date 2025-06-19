# Importations nécessaires
import pandas as pd
import psycopg2
import re
import logging
from textblob_fr import PatternTagger, PatternAnalyzer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from langdetect import detect, detect_langs, LangDetectException
from textblob import TextBlob

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': 'localhost',
    'database': 'airflow_db',
    'user': 'review_user',
    'password': 'review_user',
    'port': '5432'
}

# Initialiser les analyseurs une seule fois (global)
try:
    arabic_sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="CAMeL-Lab/bert-base-arabic-camelbert-da-sentiment",
        tokenizer="CAMeL-Lab/bert-base-arabic-camelbert-da-sentiment"
    )
    logger.info("Pipeline arabe initialisé avec succès")
except Exception as e:
    logger.error(f"Erreur lors de l'initialisation du pipeline arabe: {e}")
    arabic_sentiment_pipeline = None

# Initialiser VADER pour l'anglais (plus robuste que TextBlob seul)
vader_analyzer = SentimentIntensityAnalyzer()

# 1. Connexion à PostgreSQL
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à la base de données: {e}")
        raise

# 2. Chargement des données nettoyées depuis PostgreSQL
def load_cleaned_data():
    """Charge les données nettoyées depuis PostgreSQL"""
    try:
        conn = get_db_connection()
        query = "SELECT * FROM stg_reviews_cleaned;"
        df = pd.read_sql(query, conn)
        conn.close()
        logger.info(f"Données chargées: {len(df)} lignes")
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données: {e}")
        raise

def detect_review_language(text):
    """Détection robuste de la langue avec gestion d'erreurs améliorée"""
    if not isinstance(text, str) or len(text.strip()) < 3:
        return 'unknown'
    
    try:
        # Nettoyer le texte
        clean_text = text.strip()
        
        # Vérifier d'abord la présence de caractères arabes
        if re.search('[\u0600-\u06FF]', clean_text):
            return 'ar'
        
        # Détecter la langue avec langdetect
        detected_langs = detect_langs(clean_text)
        
        if detected_langs:
            primary_lang = detected_langs[0]
            
            # Seuil de confiance minimum
            if primary_lang.prob > 0.7:
                if primary_lang.lang in ['fr', 'en']:
                    return primary_lang.lang
                else:
                    return 'other'
            else:
                # Si confiance faible, essayer avec des mots clés
                french_indicators = ['le', 'la', 'les', 'de', 'du', 'des', 'et', 'ou', 'avec', 'sans', 'pour']
                english_indicators = ['the', 'and', 'or', 'with', 'without', 'for', 'this', 'that']
                
                text_lower = clean_text.lower()
                french_count = sum(1 for word in french_indicators if word in text_lower)
                english_count = sum(1 for word in english_indicators if word in text_lower)
                
                if french_count > english_count and french_count > 0:
                    return 'fr'
                elif english_count > 0:
                    return 'en'
                else:
                    return 'unknown'
        
        return 'unknown'
        
    except LangDetectException as e:
        logger.warning(f"Erreur détection langue pour '{text[:50]}...': {str(e)}")
        return 'unknown'
    except Exception as e:
        logger.error(f"Erreur inattendue détection langue: {str(e)}")
        return 'unknown'

def add_language_column(df, text_column='final_text'):
    """Ajoute une colonne de langue au DataFrame"""
    logger.info("Début de la détection de langue...")
    df['language'] = df[text_column].apply(detect_review_language)
    
    # Statistiques sur les langues détectées
    lang_stats = df['language'].value_counts()
    logger.info(f"Langues détectées: {dict(lang_stats)}")
    
    return df

def analyze_sentiment_french(text):
    """Analyse de sentiment spécialisée pour le français"""
    try:
        blob = TextBlob(text, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())
        polarity = blob.sentiment[0]  # PatternAnalyzer retourne (polarity, subjectivity)
        
        # Normaliser la polarité (PatternAnalyzer retourne des valeurs entre -1 et 1)
        return polarity
    except Exception as e:
        logger.warning(f"Erreur analyse sentiment français: {e}")
        return 0.0

def analyze_sentiment_english(text):
    """Analyse de sentiment pour l'anglais avec VADER + TextBlob"""
    try:
        # VADER analysis (plus robuste pour les textes courts et informels)
        vader_scores = vader_analyzer.polarity_scores(text)
        vader_compound = vader_scores['compound']
        
        # TextBlob analysis
        blob = TextBlob(text)
        textblob_polarity = blob.sentiment.polarity
        
        # Combiner les deux scores (moyenne pondérée)
        combined_score = (vader_compound * 0.6) + (textblob_polarity * 0.4)
        
        return combined_score
    except Exception as e:
        logger.warning(f"Erreur analyse sentiment anglais: {e}")
        return 0.0

def analyze_sentiment_arabic(text):
    """Analyse de sentiment pour l'arabe"""
    if arabic_sentiment_pipeline is None:
        logger.warning("Pipeline arabe non disponible")
        return 0.0
    
    try:
        # Limiter la longueur du texte pour éviter les erreurs
        max_length = 512
        if len(text) > max_length:
            text = text[:max_length]
        
        prediction = arabic_sentiment_pipeline(text)[0]
        label = prediction['label'].lower()
        score = prediction['score']
        
        # Mapper les labels vers des scores numériques
        if 'pos' in label or 'positive' in label:
            return score
        elif 'neg' in label or 'negative' in label:
            return -score
        else:  # neutral
            return 0.0
            
    except Exception as e:
        logger.warning(f"Erreur analyse sentiment arabe: {e}")
        return 0.0

def analyze_sentiment(text, language=None):
    """Analyse de sentiment robuste multi-langue"""
    if not isinstance(text, str) or len(text.strip()) < 3:
        return ('unknown', 0.0)

    try:
        clean_text = text.strip()
        
        # Détecter la langue si non fournie
        if language is None or language == 'unknown':
            language = detect_review_language(clean_text)
        
        # Analyser selon la langue
        if language == 'fr':
            polarity = analyze_sentiment_french(clean_text)
        elif language == 'en':
            polarity = analyze_sentiment_english(clean_text)
        elif language == 'ar':
            polarity = analyze_sentiment_arabic(clean_text)
        else:
            # Fallback vers l'anglais pour les langues non supportées
            polarity = analyze_sentiment_english(clean_text)
            
        # Classification avec seuils ajustés
        if polarity > 0.1:  # Seuil plus bas pour plus de sensibilité
            return ('positive', round(polarity, 3))
        elif polarity < -0.1:
            return ('negative', round(polarity, 3))
        else:
            return ('neutral', round(polarity, 3))

    except Exception as e:
        logger.error(f"Erreur analyse sentiment pour '{text[:50]}...': {str(e)}")
        return ('unknown', 0.0)

def add_sentiment_columns(df, text_column='final_text', language_column='language'):
    """Ajoute les colonnes de sentiment au DataFrame avec gestion d'erreurs"""
    logger.info("Début de l'analyse de sentiment...")
    
    def apply_analysis(row):
        try:
            text = row[text_column]
            lang = row[language_column] if language_column in row else None
            return analyze_sentiment(text, lang)
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse de la ligne {row.name}: {e}")
            return ('unknown', 0.0)

    # Analyser par chunks pour éviter les problèmes de mémoire
    chunk_size = 1000
    results_list = []
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunk_results = chunk.apply(apply_analysis, axis=1, result_type='expand')
        results_list.append(chunk_results)
        logger.info(f"Traité {min(i+chunk_size, len(df))}/{len(df)} lignes")
    
    # Combiner tous les résultats
    results = pd.concat(results_list, ignore_index=True)
    df[['sentiment', 'sentiment_score']] = results
    
    # Statistiques sur les sentiments
    sentiment_stats = df['sentiment'].value_counts()
    logger.info(f"Sentiments détectés: {dict(sentiment_stats)}")
    
    return df

def validate_sentiment_results(df):
    """Valide les résultats d'analyse de sentiment"""
    logger.info("Validation des résultats...")
    
    # Vérifier les valeurs manquantes
    missing_sentiment = df['sentiment'].isna().sum()
    missing_scores = df['sentiment_score'].isna().sum()
    
    if missing_sentiment > 0:
        logger.warning(f"{missing_sentiment} sentiments manquants")
    if missing_scores > 0:
        logger.warning(f"{missing_scores} scores manquants")
    
    # Vérifier la cohérence des scores
    positive_with_negative_score = len(df[(df['sentiment'] == 'positive') & (df['sentiment_score'] < 0)])
    negative_with_positive_score = len(df[(df['sentiment'] == 'negative') & (df['sentiment_score'] > 0)])
    
    if positive_with_negative_score > 0:
        logger.warning(f"{positive_with_negative_score} sentiments positifs avec score négatif")
    if negative_with_positive_score > 0:
        logger.warning(f"{negative_with_positive_score} sentiments négatifs avec score positif")
    
    return df

def enrich_reviews_data():
    """Pipeline complet d'enrichissement des données"""
    try:
        logger.info("Début du pipeline d'enrichissement...")
        
        # 1. Chargement des données
        df = load_cleaned_data()
        
        # 2. Détection de langue
        df = add_language_column(df)
        
        # 3. Analyse de sentiment
        df = add_sentiment_columns(df)
        
        # 4. Validation des résultats
        df = validate_sentiment_results(df)
        
        # 5. Sauvegarde
        output_path = "/home/younes/airflow/data/enriched_reviews.json"
        df.to_json(output_path, orient="records", force_ascii=False, indent=4)
        logger.info(f"Données enrichies sauvegardées: {output_path}")
        
        return df
        
    except Exception as e:
        logger.error(f"Erreur dans le pipeline d'enrichissement: {e}")
        raise

# Fonction de test pour vérifier les analyses
# def test_sentiment_analysis():
#     """Teste l'analyse de sentiment sur des exemples"""
#     test_cases = [
#         ("C'est un excellent service, je recommande vivement!", 'fr'),
#         ("This is a terrible experience, I hate it!", 'en'),
#         ("هذا منتج رائع جداً وأنصح به", 'ar'),
#         ("Service normal, rien d'exceptionnel", 'fr'),
#         ("The product is okay, nothing special", 'en')
#     ]
    
#     print("=== Test d'analyse de sentiment ===")
#     for text, lang in test_cases:
#         sentiment, score = analyze_sentiment(text, lang)
#         print(f"Texte: {text}")
#         print(f"Langue: {lang} | Sentiment: {sentiment} | Score: {score}")
#         print("-" * 50)

# if __name__ == "__main__":
#     # test_sentiment_anlysis()  # Décommenter pour tester
#     enrich_reviews_data()