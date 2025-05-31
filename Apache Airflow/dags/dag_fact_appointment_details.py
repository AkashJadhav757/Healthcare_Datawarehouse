from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from workflows.appoitments_details import run_appointment_details

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 16),
    'retries': 1
}

with DAG(
    dag_id='dag_fact_appointment_details',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:


    run_etl = PythonOperator(
        task_id='fact_appointment_details',
        python_callable=run_appointment_details,
        op_kwargs={
            'job_name': 'fact_appointment_details',
            'source_table': 'appointments',
            'target_table': 'fact_appointment_details',
            'stage_table': 'stg_appointment_details',
            'primary_key': 'appointment_id'
        }
    )

