import pandas as pd
import re
import logging
from bs4 import BeautifulSoup 
from langdetect import detect, DetectorFactory
from utils.minio_client import (
    get_json_from_bronze, 
    save_parquet_to_layer, 
    get_today_bronze_objects
)

# Configurer le logging pour voir l'avancement dans Airflow
logger = logging.getLogger(__name__)

# Fixer la graine pour que la détection de langue soit constante
DetectorFactory.seed = 0

def clean_html_content(text):
    """
    Supprime les balises HTML et normalise le texte.
    Répond à la contrainte de 'Suppression HTML' du prof.
    """
    if not text or text == "Sans titre":
        return ""
    
    try:
        # 1. Utilisation de BeautifulSoup pour extraire le texte pur
        soup = BeautifulSoup(text, "html.parser")
        clean_text = soup.get_text(separator=' ')
        
        # 2. Normalisation : Supprimer les espaces multiples, tabulations et retours à la ligne
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        return clean_text
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage HTML : {e}")
        return text

def detect_language_safe(title, content):
    """
    Détecte la langue en combinant le titre et le contenu pour plus de précision.
    Répond à la contrainte 'Détection de langue' du prof.
    """
    # On combine titre et contenu pour donner plus de texte à l'algorithme
    text_to_detect = f"{title} {content}".strip()
    
    if not text_to_detect or len(text_to_detect) < 5:
        return "unknown"
    
    try:
        return detect(text_to_detect)
    except:
        return "unknown"

def process_bronze_to_silver():
    """
    Lit les fichiers JSON de Bronze, les nettoie et les sauve en Parquet dans Silver.
    """
    logger.info("Démarrage de la transformation Bronze -> Silver...")
    
    # 1. Lister tous les fichiers JSON déposés aujourd'hui dans Bronze
    files = get_today_bronze_objects()
    
    if not files:
        logger.warning("Aucun fichier trouvé dans Bronze pour aujourd'hui.")
        return

    for file_path in files:
        logger.info(f"Traitement du fichier : {file_path}")
        
        # 2. Lire les données JSON depuis MinIO
        data = get_json_from_bronze(file_path)
        if not data:
            continue
            
        df = pd.DataFrame(data)
        initial_count = len(df)
        
        # 3. Application du nettoyage sur le Titre et le Contenu
        df['content'] = df['content'].apply(clean_html_content)
        df['title'] = df['title'].apply(clean_html_content)
        
        # 4. Détection de la langue sur (Titre + Contenu) nettoyés
        df['language'] = df.apply(lambda x: detect_language_safe(x['title'], x['content']), axis=1)
        
        # 5. Contrôle Qualité de base
        # On garde les articles qui ont au moins un titre ou un contenu
        df = df[(df['content'].str.len() > 5) | (df['title'].str.len() > 5)]
        final_count = len(df)
        
        logger.info(f"Fichier {file_path} : {initial_count} articles lus -> {final_count} articles conservés après nettoyage.")

        if final_count == 0:
            logger.warning(f"Attention : Le fichier {file_path} est vide après nettoyage et ne sera pas sauvegardé.")
            continue
        
        # 6. Sauvegarde en format Parquet dans la couche Silver
        parquet_path = file_path.replace(".json", ".parquet")
        save_parquet_to_layer(df, layer="silver", object_name=parquet_path)
        
    logger.info("Transformation Silver terminée avec succès.")

if __name__ == "__main__":
    process_bronze_to_silver()