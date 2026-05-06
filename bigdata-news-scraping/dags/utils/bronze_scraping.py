"""
bronze_scraping.py — DAG Airflow : Scraping Batch (Couche Bronze)
Sources RSS  : The Guardian, BBC News, France 24, Al Jazeera, Arab News
Sources HTML : Hespress, Goud.ma
Couche       : Bronze (données brutes JSON → MinIO)
Schedule     : Toutes les heures
"""

import sys
sys.path.append('/opt/airflow/dags')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Importer les 7 scrapers mis à jour
from utils.scrapers import (
    TheGuardianScraper,
    BBCScraper,
    LeMondeScraper,
    AlJazeeraScraper,
    RFIScraper,
    HespressRSSScraper,
    EuronewsScraper,
)

from utils.minio_client import save_json_to_bronze, init_all_buckets

# CONFIGURATION PAR DÉFAUT

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# FONCTIONS DES TÂCHES

def initialize_buckets(**context):

    """Vérifier que les 3 buckets MinIO existent."""

    print("[Init] Vérification des buckets MinIO...")
    init_all_buckets()
    print("[Init] Buckets Bronze, Silver, Gold prêts.")


def scrape_source(scraper_class, **context):

    """
    Tâche générique de scraping.
    1. Créer le scraper
    2. Appeler scrape_articles()
    3. Sauvegarder en JSON dans MinIO Bronze
    4. Passer les infos via XCom
    """

    scraper = scraper_class()
    print(f"[{scraper.source_name}] Début du scraping...")

    articles = scraper.scrape_articles()

    if not articles:
        raise ValueError(
            f"[{scraper.source_name}] Aucun article collecté ! "
            f"Vérifier si le flux RSS ou le site est accessible."
        )

    path = save_json_to_bronze(articles, scraper.source_name)

    context['ti'].xcom_push(key='bronze_path', value=path)
    context['ti'].xcom_push(key='article_count', value=len(articles))
    context['ti'].xcom_push(key='source_name', value=scraper.source_name)

    print(f"[OK] {len(articles)} articles de '{scraper.source_name}' → {path}")
    return f"{len(articles)} articles collectés depuis {scraper.source_name}"


# DÉFINITION DU DAG

with DAG(
    dag_id='01_bronze_scraping',
    default_args=default_args,
    description=(
        'Scraping batch toutes les heures — '
        'Guardian, BBC, France24, AlJazeera, ArabNews, Hespress, Goud'
    ),
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 4, 13),
    catchup=False,
    tags=['bronze', 'scraping', 'batch'],
) as dag:

# Tâche 0 : Initialisation 
    
    init_buckets = PythonOperator(
        task_id='init_minio_buckets',
        python_callable=initialize_buckets,
    )

# Tâche 1 : The Guardian (RSS)
    
    scrape_guardian = PythonOperator(
        task_id='scrape_the_guardian',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': TheGuardianScraper},
    )

# Tâche 2 : BBC News (RSS)
    
    scrape_bbc = PythonOperator(
        task_id='scrape_bbc_news',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': BBCScraper},
    )

# Tâche 3 : Le Monde (RSS) 

    scrape_le_monde = PythonOperator(
        task_id='scrape_le_monde',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': LeMondeScraper},
    )

# Tâche 4 : Al Jazeera (RSS) 
    
    scrape_aljazeera = PythonOperator(
        task_id='scrape_aljazeera',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': AlJazeeraScraper},
    )

# Tâche 5 : RFI (RSS) 
    
    scrape_rfi = PythonOperator(
        task_id='scrape_rfi',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': RFIScraper},
    )

# Tâche 6 : Hespress (RSS) 
    
    scrape_hespress = PythonOperator(
        task_id='scrape_hespress',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': HespressRSSScraper},
    )

# Tâche 7 : Euronews (RSS) 
    
    scrape_euronews = PythonOperator(
        task_id='scrape_euronews',
        python_callable=scrape_source,
        op_kwargs={'scraper_class': EuronewsScraper},
    )
    init_buckets >> [
        scrape_guardian,
        scrape_bbc,
        scrape_le_monde,
        scrape_aljazeera,
        scrape_rfi,
        scrape_hespress,
        scrape_euronews,
    ]