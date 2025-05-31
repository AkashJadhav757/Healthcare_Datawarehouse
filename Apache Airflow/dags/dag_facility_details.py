from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from workflows.facility_details import run_facility_details


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 16),
    'retries': 1
}

with DAG(
    dag_id='dag_facility_details',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # every 15 minutes
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='facility_details',
        python_callable=run_facility_details,
        op_kwargs={
            'job_name': 'dim_facility',
            'source_table': 'appointments',
            'target_table': 'dim_facility_details',
            'stage_table': 'stg_facility_details',
            'primary_key': 'facility_name'
        }
    )
