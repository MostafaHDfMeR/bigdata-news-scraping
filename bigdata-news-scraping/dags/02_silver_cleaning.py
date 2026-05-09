from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# S'assurer qu'Airflow trouve le dossier utils
sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'dags'))

from utils.silver_cleaning import process_bronze_to_silver

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='02_silver_cleaning',
    default_args=default_args,
    description='Nettoyage des données (Bronze -> Silver) avec Pandas',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['silver', 'cleaning', 'pandas'],
) as dag:

    clean_data_task = PythonOperator(
        task_id='clean_bronze_to_silver',
        python_callable=process_bronze_to_silver,
    )

    clean_data_task
