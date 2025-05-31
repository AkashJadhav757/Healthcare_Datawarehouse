from framework.ETL_Functions import *
import pandas as pd
from datetime import datetime, date
import pytz
import pymysql
from db_connections import *


ist = pytz.timezone('Asia/Kolkata')

def run_invoice_details(job_name, source_table, target_table, stage_table, primary_key):
    now = datetime.now(ist)
    try:
        last_run = get_last_etl_run_timestamp(job_name)
    except Exception as e:
        print('Failed to get last etl run details',e)
        raise

    #Extracting data from source 

    try:
        df_source = extract_new_data(source_table, last_run)
    except Exception as e:
        print('Failed to get last etl run details',e)
        raise

    try:
        insert_df, update_df = get_upsert_records(df_source)
    except Exception as e:
        print('Failed to filter insert-update records:', e)
        raise

    #Loading to Stage table

    try:
        load_to_stage(df_source, stage_table)
    except Exception as e:
        print('Failed to loading data to Stage DB',e)
        raise
    if len(update_df)>0:
        print("Deleting old records for updated ids")
        ids_to_update = update_df[f'{primary_key}'].tolist()
        format_ids = ','.join(f"'{id}'" for id in ids_to_update)
        try:
            with datawarehouse.connect():
                delete_query = f'''
                    DELETE FROM {target_table} 
                    WHERE {primary_key} IN ({format_ids});
                '''
                datawarehouse.execute(delete_query)
                print("Process Complete")
        except Exception as e:
            print(f"Error during delete: {e}")
            raise
    else:
        pass
    #Loading to target table

    try:
        load_to_target(df_source,target_table)
        log_etl_status(job_name, 'Completed', len(insert_df), len(update_df), now)
    except Exception as e:
        print(f"Error during Loading to Target table: {e}")
        log_etl_status(job_name, 'Failed', 0, 0, now)
        raise