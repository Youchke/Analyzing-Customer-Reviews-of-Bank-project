import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from bs4 import BeautifulSoup
from pathlib import Path
import time
import json
import sys
import io
import re
import random
import os
from datetime import datetime, timedelta
import logging
import traceback

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChromeDriverManager:
    """Gestionnaire robuste du driver Chrome avec récupération automatique"""
    
    def __init__(self, max_retries=3):
        self.driver = None
        self.max_retries = max_retries
        self.retry_count = 0
        
    def create_driver(self):
        """Crée une nouvelle instance du driver Chrome"""
        try:
            chromedriver_autoinstaller.install()
            
            options = webdriver.ChromeOptions()
            
            # Options de base pour la stabilité
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1280,720")  # Taille réduite
            
            # Options pour éviter les crashes
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-images")
            options.add_argument("--disable-javascript")  # Désactiver JS non essentiel
            options.add_argument("--disable-web-security")
            options.add_argument("--disable-features=VizDisplayCompositor")
            
            # Gestion mémoire améliorée
            options.add_argument("--memory-pressure-off")
            options.add_argument("--max_old_space_size=4096")
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-renderer-backgrounding")
            options.add_argument("--disable-backgrounding-occluded-windows")
            
            # Mode headless pour économiser les ressources (optionnel)
            # options.add_argument("--headless")
            
            # User agent
            options.add_argument("--user-agent=Mozilla/5.0 (Linux; Ubuntu 20.04) AppleWebKit/537.36 Chrome/120.0.0.0")
            
            # Anti-détection
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            driver = webdriver.Chrome(options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Timeouts plus courts pour éviter les blocages
            driver.set_page_load_timeout(30)
            driver.implicitly_wait(10)
            
            logger.info("✅ Nouveau driver Chrome créé avec succès")
            return driver
            
        except Exception as e:
            logger.error(f"❌ Erreur création driver: {e}")
            return None
    
    def get_driver(self):
        """Récupère le driver actuel ou en crée un nouveau"""
        if self.driver is None:
            self.driver = self.create_driver()
        return self.driver
    
    def is_driver_alive(self):
        """Vérifie si le driver est encore fonctionnel"""
        try:
            if self.driver is None:
                return False
            # Test simple pour vérifier si le driver répond
            self.driver.current_url
            return True
        except:
            return False
    
    def restart_driver(self):
        """Redémarre le driver en cas de crash"""
        logger.warning("🔄 Redémarrage du driver suite à un crash...")
        
        # Fermer l'ancien driver
        try:
            if self.driver:
                self.driver.quit()
        except:
            pass
        
        self.driver = None
        time.sleep(5)  
        
        # Créer nouveau driver
        self.driver = self.create_driver()
        return self.driver is not None
    
    def execute_with_retry(self, func, *args, **kwargs):
        """Exécute une fonction avec retry automatique en cas de crash"""
        for attempt in range(self.max_retries):
            try:
                if not self.is_driver_alive():
                    if not self.restart_driver():
                        logger.error("❌ Impossible de redémarrer le driver")
                        return None
                
                return func(self.driver, *args, **kwargs)
                
            except WebDriverException as e:
                if "tab crashed" in str(e) or "chrome not reachable" in str(e):
                    logger.warning(f"⚠ Crash détecté (tentative {attempt + 1}/{self.max_retries}): {e}")
                    if attempt < self.max_retries - 1:
                        if self.restart_driver():
                            continue
                    else:
                        logger.error("❌ Échec après toutes les tentatives de récupération")
                        return None
                else:
                    logger.error(f"❌ Erreur WebDriver: {e}")
                    return None
            except Exception as e:
                logger.error(f"❌ Erreur inattendue: {e}")
                return None
        
        return None
    
    def quit(self):
        """Ferme proprement le driver"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                logger.info("✅ Driver fermé proprement")
        except:
            logger.warning("⚠ Erreur lors de la fermeture du driver")

def parse_relative_date(relative_str):
    """Parse une date relative en français vers format ISO"""
    if not relative_str:
        return None
        
    today = datetime.today()
    relative_str = relative_str.lower().strip()
    
    # Nettoyer la chaîne
    relative_str = relative_str.replace(',', '').replace('.', '')
    
    # Patterns pour différents formats français
    patterns = [
        r"il y a (\d+)\s*(jour|jours|mois|semaine|semaines|an|ans|année|années)",
        r"il y a (une?|un)\s*(semaine|mois|an|année)",
        r"(\d+)\s*(jour|jours|mois|semaine|semaines|an|ans|année|années)",
        r"(une?|un)\s*(semaine|mois|an|année)",
        r"(hier|aujourd'hui|today|yesterday)"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, relative_str)
        if match:
            if len(match.groups()) == 1:
                special = match.group(1)
                if special in ['hier', 'yesterday']:
                    result = today - timedelta(days=1)
                elif special in ['aujourd\'hui', 'today']:
                    result = today
                else:
                    continue
            else:
                quantity_str = match.group(1)
                unit = match.group(2)
                
                if quantity_str in ['un', 'une', 'a']:
                    quantity = 1
                else:
                    try:
                        quantity = int(quantity_str)
                    except ValueError:
                        continue
                
                if unit.startswith('jour'):
                    result = today - timedelta(days=quantity)
                elif unit.startswith('semaine'):
                    result = today - timedelta(weeks=quantity)
                elif unit.startswith('mois'):
                    result = today - timedelta(days=30 * quantity)
                elif unit.startswith('an') or unit.startswith('année'):
                    result = today - timedelta(days=365 * quantity)
                else:
                    continue
            
            return result.strftime('%Y-%m-%d')
    
    return None

def safe_get_page(driver, url, max_wait=30):
    """Charge une page de manière sécurisée avec timeout"""
    try:
        logger.info(f"📄 Chargement: {url}")
        driver.get(url)
        
        # Attendre que la page soit chargée
        WebDriverWait(driver, max_wait).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        
        time.sleep(random.uniform(3.0, 5.0))
        return True
        
    except TimeoutException:
        logger.warning(f"⚠ Timeout lors du chargement de {url}")
        return False
    except Exception as e:
        logger.error(f"❌ Erreur chargement page: {e}")
        return False

def extract_branch_links_safe(driver, bank, city):
    """Version sécurisée de l'extraction des liens d'agences"""
    try:
        logger.info(f"🔍 Recherche: {bank} à {city}")
        
        search_query = f"{bank} agence {city} Maroc"
        search_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
        
        if not safe_get_page(driver, search_url):
            return []
        
        # Attendre les résultats avec timeout réduit
        try:
            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='main']")))
        except TimeoutException:
            logger.warning(f"⚠ Timeout résultats pour {bank} à {city}")
            return []
        
        # Scroll limité pour éviter les crashes
        for scroll in range(5):  # Réduire le nombre de scrolls
            try:
                driver.execute_script("window.scrollBy(0, 1000);")
                time.sleep(2)
            except:
                break
        
        # Extraction des liens
        branch_links = []
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
            for element in elements[:5]:  # Limiter à 5 liens max par recherche
                href = element.get_attribute("href")
                if href and href not in branch_links:
                    branch_links.append(href)
        except Exception as e:
            logger.warning(f"⚠ Erreur extraction liens: {e}")
        
        logger.info(f"✅ {len(branch_links)} agences trouvées pour {bank} à {city}")
        return branch_links
        
    except Exception as e:
        logger.error(f"❌ Erreur recherche {bank} à {city}: {e}")
        return []

def extract_reviews_from_branch_safe(driver, branch_url, bank_name, city):
    """Version sécurisée de l'extraction des avis"""
    try:
        logger.info(f"📖 Extraction avis: {branch_url}")
        
        if not safe_get_page(driver, branch_url):
            return []
        
        # Parser le HTML actuel
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Informations de base de l'agence
        display_name = bank_name
        name_element = soup.select_one('h1.DUwDvf.lfPIob, h1')
        if name_element:
            display_name = name_element.get_text().strip()
        
        location = f"{city} - Localisation inconnue"
        location_element = soup.select_one('div.Io6YTe.fontBodyMedium.kR99db.fdkmkc, [data-item-id="address"]')
        if location_element:
            location = location_element.get_text().strip()
        
        overall_rating = None
        rating_element = soup.select_one('div.F7nice span span, .ceNzKf')
        if rating_element:
            overall_rating = rating_element.get_text().strip()
        
        # Chercher et cliquer sur le bouton des avis
        reviews_clicked = False
        try:
            # Méthodes simplifiées pour trouver le bouton
            button_selectors = [
                "button[aria-label*='avis']",
                "button[data-value='reviews']",
                "*[jsaction*='pane.rating']"
            ]
            
            for selector in button_selectors:
                try:
                    button = driver.find_element(By.CSS_SELECTOR, selector)
                    if button.is_displayed():
                        driver.execute_script("arguments[0].click();", button)
                        reviews_clicked = True
                        logger.info("✅ Bouton avis cliqué")
                        break
                except:
                    continue
        except:
            pass
        
        if reviews_clicked:
            time.sleep(5)  # Attendre le chargement des avis
            
            # Scroll limité dans les avis
            try:
                for _ in range(3):  # Scroll limité
                    driver.execute_script("window.scrollBy(0, 500);")
                    time.sleep(2)
            except:
                pass
        
        # Re-parser après les actions
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extraction simplifiée des avis
        reviews = []
        review_elements = soup.select('div.jftiEf.fontBodyMedium, div[data-review-id]')
        
        for i, review_element in enumerate(review_elements[:10]):  # Limiter à 10 avis
            try:
                # Date
                date_element = review_element.select_one('span.rsqaWe, .DU9Pgb')
                review_date = None
                if date_element:
                    review_date = parse_relative_date(date_element.get_text().strip())
                
                # Texte
                text_element = review_element.select_one('span.wiI7pd, .MyEned')
                review_text = None
                if text_element:
                    review_text = text_element.get_text().strip()
                
                # Rating (nombre d'étoiles)
                stars = review_element.select('span.elGi1d')
                review_rating = len(stars) if stars else None
                
                if review_text or review_rating:  # Au moins un élément doit être présent
                    reviews.append({
                        "bank_name": display_name,
                        "branch_name": f"{display_name} - {location}",
                        "bank_rating": overall_rating,
                        "bank_location": location,
                        "review_date": review_date,
                        "review_text": review_text,
                        "review_rating": review_rating
                    })
                
            except Exception as e:
                logger.warning(f"⚠ Erreur avis {i+1}: {e}")
                continue
        
        logger.info(f"✅ {len(reviews)} avis extraits")
        return reviews
        
    except Exception as e:
        logger.error(f"❌ Erreur extraction avis: {e}")
        return []

def extract():
    """Fonction principale avec gestion robuste des crashes"""
    cities = [
        "Agadir", "Casablanca", "Rabat", "Tanger", 
        "Marrakech", "Kenitra", "Tata"
    ]

    banks = [
        "Attijariwafa Bank", "AL BARID BANK", "BANQUE POPULAIRE",
        "BANK OF AFRICA", "BMCI BANK", "CIH BANK"
    ]
    
    # Configuration des fichiers
    output_dir = Path('/home/younes/airflow/data')
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / 'all_reviews2.json'
    
    if file_path.exists():
        file_path.unlink()
    
    # Initialiser le gestionnaire de driver
    driver_manager = ChromeDriverManager(max_retries=3)
    all_reviews = []
    
    try:
        logger.info("🚀 Début de l'extraction avec gestion anti-crash")
        
        for city in cities:
            logger.info(f"\n{'='*60}")
            logger.info(f"🌍 VILLE: {city}")
            logger.info(f"{'='*60}")
            
            for bank in banks:
                logger.info(f"\n🏦 {bank} à {city}")
                
                # Extraction des liens avec retry automatique
                branch_links = driver_manager.execute_with_retry(
                    extract_branch_links_safe, bank, city
                )
                
                if not branch_links:
                    logger.warning(f"⚠ Aucune agence pour {bank} à {city}")
                    continue
                
                # Traiter chaque agence (limité à 2 pour éviter les crashes)
                for i, branch_url in enumerate(branch_links[:2]):
                    logger.info(f"--- Agence {i+1}/{len(branch_links[:2])} ---")
                    
                    reviews = driver_manager.execute_with_retry(
                        extract_reviews_from_branch_safe, branch_url, bank, city
                    )
                    
                    if reviews:
                        all_reviews.extend(reviews)
                        
                        # Sauvegarde incrémentale
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(all_reviews, f, indent=2, ensure_ascii=False)
                        
                        logger.info(f"✅ +{len(reviews)} avis (Total: {len(all_reviews)})")
                    
                    # Pause entre agences
                    time.sleep(random.uniform(8.0, 12.0))
                
                # Pause entre banques
                time.sleep(random.uniform(10.0, 15.0))
            
            logger.info(f"✅ VILLE {city} TERMINÉE - Total: {len(all_reviews)} avis")
        
        # Statistiques finales
        total_with_text = sum(1 for r in all_reviews if r.get('review_text'))
        logger.info(f"🎉 EXTRACTION TERMINÉE!")
        logger.info(f"📊 Total: {len(all_reviews)} avis ({total_with_text} avec texte)")
        
    except Exception as e:
        logger.error(f"❌ Erreur critique: {e}")
        traceback.print_exc()
        
    finally:
        # Fermeture propre
        driver_manager.quit()
        
        # Sauvegarde finale
        if all_reviews:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(all_reviews, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Données sauvegardées: {file_path}")

# if __name__ == "__main__":
#     extract()