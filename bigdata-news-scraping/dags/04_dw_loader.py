"""
04_dw_loader.py — DAG Airflow : Chargement Gold → Data Warehouse
Couche    : Data Warehouse (postgres_dw / news_dw)
Schedule  : @daily — après le DAG 03_gold_analytics
Tâche     : Charger les 5 tables Gold dans PostgreSQL
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# S'assurer qu'Airflow trouve le dossier utils
sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags'))

from utils.dw_loader import load_gold_to_dw

# CONFIGURATION PAR DÉFAUT

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# DÉFINITION DU DAG

with DAG(
    dag_id='04_dw_loader',
    default_args=default_args,
    description='Chargement Gold → Data Warehouse PostgreSQL (5 tables KPI)',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['gold', 'datawarehouse', 'postgresql'],
) as dag:

    load_to_dw = PythonOperator(
        task_id='load_gold_to_dw',
        python_callable=load_gold_to_dw,
    )

    load_to_dw
