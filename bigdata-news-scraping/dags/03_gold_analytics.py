"""
03_gold_analytics.py — DAG Airflow : Transformation Silver -> Gold
KPIs   :
    1> articles_by_source
    2> articles_by_language
    3> articles_by_day
    4> top_categories_by_source
    5> top_sources_by_language
Schedule : Quotidien (@daily) — après le DAG Silver
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# S'assurer qu'Airflow trouve le dossier utils
sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags'))

from utils.gold_analytics import process_silver_to_gold

#CONFIGURATION PAR DÉFAUT

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

#DÉFINITION DU DAG

with DAG(
    dag_id='03_gold_analytics',
    default_args=default_args,
    description='Transformation Silver → Gold : 5 KPIs analytiques',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['gold', 'analytics', 'medallion'],
) as dag:

    # Tâche unique : calculer les 5 KPIs et sauvegarder dans Gold
    compute_gold_kpis = PythonOperator(
        task_id='compute_gold_kpis',
        python_callable=process_silver_to_gold,
    )

    compute_gold_kpis