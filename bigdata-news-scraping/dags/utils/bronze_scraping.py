"""
bronze_scraping.py — DAG Airflow : Scraping Batch (Couche Bronze)
Sources  : CNN, BBC News, Hesport, Al Jazeera, NBC News, Reuters, Morocco World News
Couche   : Bronze (données brutes JSON --> MinIO)
Schedule : Toutes les heures
"""

import sys
sys.path.append('/opt/airflow/dags')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Importer les 7 scrapers depuis scrapers.py
from utils.scrapers import (
    CNNScraper,
    BBCScraper,
    HesportScraper,
    AlJazeeraScraper,
    NBCNewsScraper,
    ReutersScraper,
    MoroccoWorldNewsScraper,
)

# Importer le client MinIO pour sauvegarder en Bronze
from utils.minio_client import save_json_to_bronze, init_all_buckets

# CONFIGURATION PAR DÉFAUT DES TÂCHES

default_args = {
    'owner': 'data_engineer',      # Responsable du DAG
    'depends_on_past': False,       # Ne pas attendre le run précédent
    'email_on_failure': False,      # Pas d'email en cas d'échec
    'retries': 3,                   # Réessayer 3 fois si échec
    'retry_delay': timedelta(minutes=5),  # Attendre 5 min entre chaque essai
}

# FONCTIONS DES TÂCHES

def initialize_buckets(**context):
    """
    Tâche 0 : Vérifier que les 3 buckets MinIO existent.
    S'exécute avant tous les scrapers.
    Utilise init_all_buckets() de minio_client.py.
    """
    print("[Init] Vérification des buckets MinIO...")
    init_all_buckets()
    print("[Init] Buckets Bronze, Silver, Gold prêts.")

def scrape_source(scraper_class, **context):
    # Créer le scraper correspondant à la source
    scraper = scraper_class()

    print(f"[{scraper.source_name}] Début du scraping...")

    # Lancer le scraping
    articles = scraper.scrape_articles()

    # Vérification qu'on a bien collecté des articles
    if not articles:
        raise ValueError(
            f"[{scraper.source_name}] Aucun article collecté ! "
            f"Vérifier si le site est accessible."
        )

    # Sauvegarder en JSON dans MinIO bucket Bronze
    path = save_json_to_bronze(articles, scraper.source_name)

    # Passer les informations aux tâches suivantes via XCom
    # XCom = système de communication entre tâches Airflow
    context['ti'].xcom_push(key='bronze_path', value=path)
    context['ti'].xcom_push(key='article_count', value=len(articles))
    context['ti'].xcom_push(key='source_name', value=scraper.source_name)

    print(f"[OK] {len(articles)} articles de '{scraper.source_name}' → {path}")
    return f"{len(articles)} articles collectés depuis {scraper.source_name}"

# DÉFINITION DU DAG

with DAG(
    dag_id='01_bronze_scraping',
    default_args=default_args,
    description='Scraping batch toutes les heures — CNN, BBC, Hesport, AlJazeera, NBC, Reuters, MWN',
    schedule_interval=timedelta(hours=1),   # S'exécute toutes les heures
    start_date=datetime(2026, 4, 13),
    catchup=False,   # Ne pas rattraper les runs passés
    tags=['bronze', 'scraping', 'batch'],
) as dag:

    # ── Tâche 0 : Initialisation des buckets ────────────────────────────────
    # S'exécute en premier avant tous les scrapers
    init_buckets = PythonOperator(
        task_id='init_minio_buckets',
        python_callable=initialize_buckets,
    )

    # ── Tâche 1 : Scraping CNN ───────────────────────────────────────────────
    scrape_cnn = PythonOperator(
        task_id='scrape_cnn',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': CNNScraper},
    )

    # ── Tâche 2 : Scraping BBC News ──────────────────────────────────────────
    scrape_bbc = PythonOperator(
        task_id='scrape_bbc_news',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': BBCScraper},
    )

    # ── Tâche 3 : Scraping Hesport ───────────────────────────────────────────
    scrape_hesport = PythonOperator(
        task_id='scrape_hesport',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': HesportScraper},
    )

    # ── Tâche 4 : Scraping Al Jazeera ───────────────────────────────────────
    scrape_aljazeera = PythonOperator(
        task_id='scrape_aljazeera',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': AlJazeeraScraper},
    )

    # ── Tâche 5 : Scraping NBC News ──────────────────────────────────────────
    scrape_nbc = PythonOperator(
        task_id='scrape_nbc_news',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': NBCNewsScraper},
    )

    # ── Tâche 6 : Scraping Reuters ───────────────────────────────────────────
    scrape_reuters = PythonOperator(
        task_id='scrape_reuters',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': ReutersScraper},
    )

    # ── Tâche 7 : Scraping Morocco World News ───────────────────────────────
    scrape_mwn = PythonOperator(
        task_id='scrape_morocco_world_news',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': MoroccoWorldNewsScraper},
    )

    # ── Ordre d'exécution ────────────────────────────────────────────────────
    # init_buckets s'exécute EN PREMIER
    # puis les 7 scrapers s'exécutent EN PARALLÈLE

    init_buckets >> [
        scrape_cnn,
        scrape_bbc,
        scrape_hesport,
        scrape_aljazeera,
        scrape_nbc,
        scrape_reuters,
        scrape_mwn,
    ]