from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from workflows.patient_details import run_patient_details

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 16),
    'retries': 1
}

with DAG(
    dag_id='dag_patient_details',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # every 15 minutes
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='patient_details',
        python_callable=run_patient_details,
        op_kwargs={
            'job_name': 'dim_patients',
            'source_table': 'patients',
            'target_table': 'dim_patient_details',
            'stage_table': 'stg_patients',
            'primary_key': 'patient_id'
        }
    )

