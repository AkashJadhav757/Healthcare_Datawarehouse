from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from workflows.service_provider import run_service_provider


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 17),
    'retries': 1
}


with DAG(
    dag_id='dag_service_provider',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # every 15 minutes
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='service_provider',
        python_callable=run_service_provider,
        op_kwargs={
            'job_name': 'dim_doctors',
            'source_table': 'doctors',
            'target_table': 'dim_service_provider',
            'stage_table': 'stg_service_provider',
            'primary_key': 'service_provider_id'
        }
    )
