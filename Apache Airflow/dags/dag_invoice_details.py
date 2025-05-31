from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from workflows.invoice_details import run_invoice_details

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 16),
    'retries': 1
}

with DAG(
    dag_id='dag_invoice_details',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # every 15 minutes
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='patient_details',
        python_callable=run_invoice_details,
        op_kwargs={
            'job_name': 'etl_invoice',
            'source_table': 'invoice',
            'target_table': 'invoice_details',
            'stage_table': 'stg_invoice_details',
            'primary_key': 'invoice_id'
        }
    )

