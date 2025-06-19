import pandas as pd
import numpy as np
import re
import nltk
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import logging
import warnings
import json
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration simplifiée
INPUT_JSON_FILE = '/home/younes/airflow/data/enriched_reviews.json'

# Configuration PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'database': 'airflow_db',
    'user': 'review_user',
    'password': 'review_user',
    'port': '5432'
}

class TopicExtractor:
    """Extracteur de topics utilisant LDA avec optimisation automatique."""
    
    def __init__(self, language="french"):
        self.language = language
        self.lemmatizer = WordNetLemmatizer()
        self.lda_model = None
        self.vectorizer = None
        self.topic_labels = {}
        self.optimal_topics = None
        
        # Mots vides spécialisés pour le domaine bancaire
        self.domain_stopwords = {
            "french": [
                "banque", "bank", "client", "customer", "compte", "account", "service",
                "conseiller", "agence", "euro", "euros", "france", "français", "très",
                "plus", "bien", "faire", "avoir", "être", "tout", "dit", "peut",
                "merci", "bonjour", "cordialement", "madame", "monsieur"
            ],
            "english": [
                "bank", "service", "customer", "client", "account", "good", "bad",
                "great", "nice", "really", "much", "many", "also", "get", "go",
                "thank", "hello", "regards", "madam", "sir"
            ]
        }
        
        # Mapping pour des labels plus explicites
        self.topic_mappings = {
            "service": "Service_Quality",
            "staff": "Staff_Performance", 
            "conseiller": "Advisor_Service",
            "personnel": "Staff_Interaction",
            "accueil": "Customer_Welcome",
            "attente": "Wait_Time",
            "credit": "Credit_Services",
            "carte": "Card_Services",
            "frais": "Fees_Charges",
            "application": "Mobile_App",
            "agence": "Branch_Experience",
            "probleme": "Technical_Issues",
            "satisfaction": "Customer_Satisfaction"
        }
        
        self._setup_nltk()
    
    def _setup_nltk(self):
        """Configuration des ressources NLTK nécessaires."""
        resources = ["punkt", "stopwords", "wordnet", "averaged_perceptron_tagger"]
        
        for resource in resources:
            try:
                nltk.data.find(f"tokenizers/{resource}" if resource == "punkt" else f"corpora/{resource}")
            except LookupError:
                logger.info(f"Téléchargement de la ressource NLTK : {resource}")
                nltk.download(resource, quiet=True)
    
    def _preprocess_text(self, text):
        """Préprocessing du texte pour l'extraction de topics."""
        if not isinstance(text, str) or not text.strip():
            return ""
        
        # Normalisation
        text = text.lower().strip()
        
        # Nettoyage - garder lettres et quelques caractères
        text = re.sub(r"[^a-zA-ZàâäéèêëïîôöûüÿçÀÂÄÉÈÊËÏÎÔÖÛÜŸÇ\s]", " ", text)  # Caractères français inclus
        text = re.sub(r"\s+", " ", text).strip()
        
        # Tokenisation
        try:
            tokens = word_tokenize(text, language=self.language)
        except:
            tokens = text.split()
        
        # Mots vides - Version réduite pour garder plus de mots
        stop_words = set()
        try:
            stop_words.update(stopwords.words(self.language))
        except:
            logger.warning(f"Impossible de charger les stopwords pour '{self.language}'")
        
        # Ajouter seulement les mots vides les plus courants du domaine
        domain_stops_reduced = [
            "banque", "bank", "client", "très", "plus", "bien", "faire", "avoir", "être"
        ]
        stop_words.update(domain_stops_reduced)
        
        # Filtrage et lemmatisation - Critères moins stricts
        processed_tokens = []
        for token in tokens:
            if len(token) >= 2 and token.isalpha() and token not in stop_words:  # Réduit de 3 à 2
                try:
                    lemmatized = self.lemmatizer.lemmatize(token)
                    processed_tokens.append(lemmatized)
                except:
                    processed_tokens.append(token)
        
        result = " ".join(processed_tokens)
        return result
    
    def _prepare_corpus(self, df, text_column="final_text"):
        """Préparation du corpus pour l'analyse LDA."""
        logger.info("Préparation du corpus...")
        
        if text_column not in df.columns:
            raise ValueError(f"Colonne '{text_column}' introuvable.")
        
        df = df.copy()
        df[text_column] = df[text_column].astype(str).fillna("")
        
        # Filtrage des textes valides - critères plus souples
        valid_mask = (df[text_column].str.len() >= 10)  # Réduit de 20 à 10
        df_clean = df[valid_mask].copy().reset_index(drop=True)
        
        if len(df_clean) == 0:
            logger.warning("Aucun document valide trouvé.")
            return df_clean, []
        
        logger.info(f"Documents après filtrage : {len(df_clean)}/{len(df)}")
        
        # Preprocessing
        processed_texts = df_clean[text_column].apply(self._preprocess_text)
        df_clean["processed_text"] = processed_texts
        
        # Diagnostics du preprocessing
        empty_after_processing = sum(1 for text in processed_texts if len(text.strip()) == 0)
        logger.info(f"Textes vides après preprocessing : {empty_after_processing}/{len(processed_texts)}")
        
        # Filtrage final - critères plus souples
        final_mask = processed_texts.apply(lambda x: len(x.split()) >= 2)  # Réduit de 5 à 2
        df_final = df_clean[final_mask].reset_index(drop=True)
        final_texts = df_final["processed_text"].tolist()
        
        logger.info(f"Corpus final : {len(df_final)} documents")
        
        # Afficher quelques exemples de textes traités
        if final_texts:
            logger.info("Exemples de textes preprocessés :")
            for i, text in enumerate(final_texts[:3]):
                logger.info(f"  {i+1}: {text[:100]}...")
        
        return df_final, final_texts
    
    def _find_optimal_topics(self, texts, max_topics=15, min_topics=3):
        """Trouve le nombre optimal de topics basé sur la perplexité et la cohérence."""
        if not texts:
            self.optimal_topics = min_topics
            return min_topics
        
        logger.info("Recherche du nombre optimal de topics...")
        
        # Vectorisation pour les tests
        test_vectorizer = CountVectorizer(
            max_df=0.80, min_df=5, max_features=1500,
            ngram_range=(1, 2), token_pattern=r"\b[a-zA-Z][a-zA-Z]+\b"
        )
        
        try:
            doc_term_matrix = test_vectorizer.fit_transform(texts)
        except ValueError:
            self.optimal_topics = min_topics
            return min_topics
        
        # Ajuster la plage selon les données
        actual_max_topics = min(max_topics, doc_term_matrix.shape[0] // 5, doc_term_matrix.shape[1] // 10)
        if actual_max_topics < min_topics:
            self.optimal_topics = min_topics
            return min_topics
        
        logger.info(f"Test de {min_topics} à {actual_max_topics} topics...")
        
        # Test de différents nombres de topics
        perplexities = []
        valid_topic_nums = []
        
        for n_topics in range(min_topics, actual_max_topics + 1):
            try:
                lda = LatentDirichletAllocation(
                    n_components=n_topics, 
                    learning_method="online",
                    max_iter=20, 
                    random_state=42, 
                    n_jobs=-1,
                    doc_topic_prior=0.1,
                    topic_word_prior=0.01
                )
                
                lda.fit(doc_term_matrix)
                perplexity = lda.perplexity(doc_term_matrix)
                
                if np.isfinite(perplexity):
                    perplexities.append(perplexity)
                    valid_topic_nums.append(n_topics)
                    logger.info(f"  {n_topics} topics: perplexité = {perplexity:.2f}")
                
            except Exception as e:
                logger.warning(f"Erreur pour {n_topics} topics: {e}")
        
        # Sélection du nombre optimal
        if perplexities:
            # Méthode du coude : chercher le point où la diminution ralentit
            if len(perplexities) >= 3:
                # Calculer les différences relatives
                perp_array = np.array(perplexities)
                improvements = np.diff(perp_array) / perp_array[:-1] * 100
                
                # Trouver le point où l'amélioration devient marginale (< 5%)
                for i, improvement in enumerate(improvements):
                    if abs(improvement) < 5.0:  # Moins de 5% d'amélioration
                        optimal_n_topics = valid_topic_nums[i]
                        break
                else:
                    # Si pas de plateau trouvé, prendre le minimum
                    optimal_n_topics = valid_topic_nums[np.argmin(perplexities)]
            else:
                optimal_n_topics = valid_topic_nums[np.argmin(perplexities)]
        else:
            optimal_n_topics = min_topics
        
        logger.info(f"✅ Nombre optimal de topics sélectionné : {optimal_n_topics}")
        self.optimal_topics = optimal_n_topics
        return optimal_n_topics
    
    def _train_lda_model(self, texts):
        """Entraînement du modèle LDA final."""
        if not texts:
            logger.error("Liste de textes vide.")
            return None
        
        logger.info("Entraînement du modèle LDA...")
        
        # Vectorisation
        self.vectorizer = CountVectorizer(
            max_df=0.85, min_df=3, max_features=1500,
            ngram_range=(1, 2), token_pattern=r"\b[a-zA-Z][a-zA-Z]+\b"
        )
        
        try:
            doc_term_matrix = self.vectorizer.fit_transform(texts)
            logger.info(f"Matrice : {doc_term_matrix.shape[0]} documents, {doc_term_matrix.shape[1]} features")
        except ValueError as e:
            logger.error(f"Erreur de vectorisation : {e}")
            return None
        
        # Utiliser le nombre optimal de topics trouvé
        if self.optimal_topics is None:
            n_topics = max(3, min(8, doc_term_matrix.shape[0] // 30))
            self.optimal_topics = n_topics
        else:
            n_topics = min(self.optimal_topics, doc_term_matrix.shape[1])
        
        logger.info(f"Entraînement avec {n_topics} topics...")
        
        # Modèle LDA - Paramètres optimisés
        self.lda_model = LatentDirichletAllocation(
            n_components=n_topics, 
            doc_topic_prior=0.1, 
            topic_word_prior=0.01,
            learning_method="batch", 
            max_iter=50, 
            random_state=42, 
            n_jobs=-1
        )
        
        try:
            self.lda_model.fit(doc_term_matrix)
        except Exception as e:
            logger.error(f"Erreur d'entraînement LDA : {e}")
            return None
        
        # Métriques
        try:
            perplexity = self.lda_model.perplexity(doc_term_matrix)
            logger.info(f"Modèle entraîné - Perplexité: {perplexity:.2f}")
        except:
            pass
        
        self._generate_topic_labels()
        return doc_term_matrix
    
    def _generate_topic_labels(self, top_words=10):
        """Génération de labels pour les topics."""
        if self.lda_model is None or self.vectorizer is None:
            return
        
        logger.info("Génération des labels de topics...")
        self.topic_labels = {}
        feature_names = self.vectorizer.get_feature_names_out()
        
        for topic_idx, topic_weights in enumerate(self.lda_model.components_):
            top_words_indices = topic_weights.argsort()[-top_words:][::-1]
            top_words_list = [feature_names[i] for i in top_words_indices]
            
            # Création du label
            label = self._create_topic_label(top_words_list)
            self.topic_labels[topic_idx] = label
            
            logger.info(f"Topic {topic_idx}: {label}")
            logger.info(f"  Mots-clés: {', '.join(top_words_list[:5])}")
    
    def _create_topic_label(self, top_words):
        """Création d'un label significatif."""
        if not top_words:
            return "Unknown_Topic"
        
        # Recherche dans le mapping
        for word in top_words[:5]:
            for key, label in self.topic_mappings.items():
                if key.lower() in word.lower():
                    return label
        
        # Label basé sur les mots principaux
        significant_words = [w for w in top_words[:3] if len(w) > 3]
        if len(significant_words) >= 2:
            return f"{significant_words[0].title()}_{significant_words[1].title()}"
        elif significant_words:
            return significant_words[0].title()
        else:
            return f"{top_words[0].title()}_{top_words[1].title()}" if len(top_words) > 1 else top_words[0].title()
    
    def _assign_topics(self, df, doc_term_matrix):
        """Attribution des topics aux documents."""
        if self.lda_model is None or doc_term_matrix is None:
            logger.error("Modèle non entraîné.")
            df['topic_id'] = -1
            df['topic_name'] = 'Model_Error'
            df['topic_confidence'] = 0.0
            return df
        
        logger.info("Attribution des topics...")
        
        try:
            doc_topic_dist = self.lda_model.transform(doc_term_matrix)
        except Exception as e:
            logger.error(f"Erreur d'attribution : {e}")
            df['topic_id'] = -1
            df['topic_name'] = 'Assignment_Error'
            df['topic_confidence'] = 0.0
            return df
        
        df = df.copy()
        df['topic_id'] = doc_topic_dist.argmax(axis=1)
        df['topic_name'] = df['topic_id'].apply(lambda x: self.topic_labels.get(x, f"Topic_{x}"))
        df['topic_confidence'] = doc_topic_dist.max(axis=1).round(4)
        
        logger.info(f"Confiance moyenne : {df['topic_confidence'].mean():.3f}")
        
        # Distribution des topics
        topic_counts = df['topic_name'].value_counts()
        for topic, count in topic_counts.items():
            percentage = (count / len(df)) * 100
            logger.info(f"  {topic}: {count} documents ({percentage:.1f}%)")
        
        return df
    
    def _save_to_postgres(self, df_final):
        """Sauvegarde simplifiée dans PostgreSQL."""
        try:
            # Créer la connexion
            connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(connection_string)
            
            # S'assurer que review_date est au bon format
            if 'review_date' in df_final.columns:
                df_final['review_date'] = pd.to_datetime(df_final['review_date']).dt.strftime('%Y-%m-%d')
            
            # Sauvegarder dans PostgreSQL
            df_final.to_sql('raw_reviews_brutes', engine, if_exists='replace', index=False)
            
            logger.info(f"✅ {len(df_final)} enregistrements sauvegardés dans raw_reviews_brutes")
            
            # Afficher un aperçu des colonnes sauvegardées
            logger.info(f"Colonnes sauvegardées : {list(df_final.columns)}")
            
        except Exception as e:
            logger.error(f"❌ Erreur de sauvegarde PostgreSQL : {e}")
    
    def analyze(self, text_column='final_text', id_column='id', language='french'):
        """Pipeline complet d'extraction de topics."""
        self.language = language
        logger.info(f"--- Analyse des topics ({language}) ---")
        
        # Chargement des données depuis le fichier JSON
        try:
            df_original = pd.read_json(INPUT_JSON_FILE)
            logger.info(f"{len(df_original)} enregistrements chargés depuis {INPUT_JSON_FILE}")
            
            # Afficher les colonnes disponibles pour debug
            logger.info(f"Colonnes disponibles : {list(df_original.columns)}")
            
            # Diagnostics des données
            if text_column in df_original.columns:
                non_null_count = df_original[text_column].notna().sum()
                avg_length = df_original[text_column].astype(str).str.len().mean()
                logger.info(f"Textes non-vides : {non_null_count}/{len(df_original)}")
                logger.info(f"Longueur moyenne des textes : {avg_length:.1f} caractères")
            
        except Exception as e:
            logger.error(f"Erreur de chargement du fichier JSON : {e}")
            return
        
        # Vérifier si la colonne de texte existe
        if text_column not in df_original.columns:
            logger.error(f"Colonne '{text_column}' non trouvée. Colonnes disponibles : {list(df_original.columns)}")
            return
        
        # Préparation du corpus
        df_processed, processed_texts = self._prepare_corpus(df_original, text_column)
        
        if not processed_texts:
            logger.error("Corpus vide après preprocessing.")
            return
        
        # OPTIMISATION AUTOMATIQUE DU NOMBRE DE TOPICS
        self._find_optimal_topics(processed_texts, max_topics=15, min_topics=3)
        
        # Entraînement LDA avec le nombre optimal
        doc_term_matrix = self._train_lda_model(processed_texts)
        
        if self.lda_model is None:
            logger.error("Échec de l'entraînement LDA.")
            return
        
        # Attribution des topics
        df_with_topics = self._assign_topics(df_processed, doc_term_matrix)
        
        # Fusion avec les données originales
        if id_column in df_with_topics.columns and id_column in df_original.columns:
            topic_columns = ['topic_id', 'topic_name', 'topic_confidence']
            df_topics_merge = df_with_topics[[id_column] + topic_columns].copy()
            df_final = df_original.merge(df_topics_merge, on=id_column, how='left')
        else:
            # Si pas d'ID, utiliser l'index
            df_final = df_original.copy()
            min_len = min(len(df_original), len(df_with_topics))
            df_final = df_final.iloc[:min_len].copy()
            
            for col in ['topic_id', 'topic_name', 'topic_confidence']:
                if col in df_with_topics.columns:
                    df_final[col] = df_with_topics[col].iloc[:min_len].values
        
        # Remplir les valeurs manquantes
        df_final['topic_confidence'] = df_final['topic_confidence'].fillna(0.0)
        df_final['topic_name'] = df_final['topic_name'].fillna('Unclassified')
        df_final['topic_id'] = df_final['topic_id'].fillna(-1).astype(int)
        
        # Sauvegarde dans PostgreSQL
        self._save_to_postgres(df_final)
        
        logger.info("--- Analyse terminée ---")


# Point d'entrée
def main():
    extractor = TopicExtractor(language="french")
    extractor.analyze(text_column="final_text", id_column="id", language="french")

# if __name__ == "__main__":
#     main()