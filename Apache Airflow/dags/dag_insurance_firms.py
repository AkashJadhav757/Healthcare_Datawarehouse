from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from workflows.insurance_firms import run_insurance_firms

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 16),
    'retries': 1
}

with DAG(
    dag_id='dag_insurance_firms',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # every 15 minutes
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='insurance_firms',
        python_callable=run_insurance_firms,
        op_kwargs={
            'job_name': 'dim_insurance_firms',
            'source_table': 'invoice',
            'target_table': 'dim_insurance_firms',
            'stage_table': 'stg_insurance_firms'
        }
    )


