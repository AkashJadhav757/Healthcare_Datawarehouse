from sqlalchemy.engine import create_engine
import pandas as pd
import datetime
from datetime import date
from sqlalchemy.dialects.oracle import DATE, VARCHAR2, FLOAT, NUMBER,TIMESTAMP
from sqlalchemy import text,String,types
from datetime import datetime
import urllib.parse
from db_connections import *



def get_last_etl_run_timestamp(job_name: str) -> datetime:
    query = f'''
        SELECT last_run_timestamp 
        FROM stage_integration.etl_job_tracking 
        WHERE status = 'Completed' AND job_name = '{job_name}'
        ORDER BY last_run_timestamp DESC 
        LIMIT 1;
    '''
    df = pd.read_sql(query, stage_integration)
    
    if df.empty or df['last_run_timestamp'].isnull().all():
        last_run = datetime(2000, 1, 1)
        print('last ETL run NOT FOUND, Performing full load')
    else:
        last_run = df.iloc[0]['last_run_timestamp']
        print(f'Last successful ETL run for {job_name}: {last_run}')
    return last_run

def extract_new_data(source_table: str, last_run: datetime):
    query = f'''
        SELECT * FROM {source_table} 
        WHERE created_at > '{last_run}' OR updated_at > '{last_run}';
    '''
    df = pd.read_sql(query, source)
    print(f'Data Extracted successfully from {source_table},total {len(df)} records')
    return df

def get_dim_data(dim_table):
    query = f'''SELECT * FROM {dim_table};'''
    df = pd.read_sql(query, datawarehouse)
    print(f'Data read successfully from {dim_table}')
    return df


def get_upsert_records(df: pd.DataFrame):
    mask_update = df['updated_at'].notna()
    update_records = df[mask_update]
    insert_records = df[~mask_update]
    print(f'Records to INSERT: {len(insert_records)}')
    print(f'Records to UPDATE: {len(update_records)}')
    return insert_records, update_records


def load_to_stage(df: pd.DataFrame, table_name: str):
    try:
        df.to_sql(table_name, stage_db, if_exists='replace', index=False)
        print(f'Successfully loaded {len(df)} records to {table_name}')
    except Exception as e:
        print("Error loading to stage:", e)
        raise

def load_to_target(df, table_name):
    try:
        df.to_sql(table_name, datawarehouse, if_exists='append', index=False)
        print(f'{len(df)} records successfully loaded into {table_name}')
    except Exception as e:
        print("Error loading to dimension table:", e)
        raise

def log_etl_status(job_name, status, inserted, updated, run_time):
    
    query = f'''
        INSERT INTO stage_integration.etl_job_tracking 
        (job_name, last_run_timestamp, status, row_inserted, row_updated, created_at)
        VALUES ('{job_name}', '{run_time}', '{status}', {inserted}, {updated}, '{run_time}')
    '''
    with stage_integration.connect():
        stage_integration.execute(query)
        print(f'ETL log updated with status: {status}')