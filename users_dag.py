# users_dags.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow_settings import upload_raw_data_to_bronze, process_bronze_to_silver, process_silver_to_gold

# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='A simple data processing pipeline using local storage',
    schedule_interval=timedelta(days=1),
) as dag:

    upload = PythonOperator(
        task_id='upload_raw_data_to_bronze',
        python_callable=upload_raw_data_to_bronze,
    )

    process_to_silver = PythonOperator(
        task_id='process_bronze_to_silver',
        python_callable=process_bronze_to_silver,
    )

    process_to_gold = PythonOperator(
        task_id='process_silver_to_gold',
        python_callable=process_silver_to_gold,
    )

    upload >> process_to_silver >> process_to_gold
