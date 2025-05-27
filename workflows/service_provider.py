from framework.ETL_Functions import *
import pandas as pd
from datetime import datetime, date
import pytz
import pymysql
from db_connections import *


ist = pytz.timezone('Asia/Kolkata')

def run_service_provider(job_name, source_table, target_table, stage_table, primary_key):
    now = datetime.now(ist)
    try:
        last_run = get_last_etl_run_timestamp(job_name)
    except Exception as e:
        print('Failed to get last etl run details',e)
        raise

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


    try:
        #Casting columns before loading to stage table
        df_source = df_source.astype({
            'joining_date':'datetime64[ns]',
            'created_at':'datetime64[ns]',
            'updated_at':'datetime64[ns]'})
    except Exception as e:
        print('Failed to filter insert-update records:', e)
        raise

    try:
        load_to_stage(df_source, stage_table)
    except Exception as e:
        print('Failed to loading data to Stage DB',e)
        raise    

    try:
        df_renamed = df_source.rename(columns={
        'name': 'service_provider_name',
        'specialization': 'service_provider_specialization',
        'phone': 'service_provider_phone',
        'email': 'service_provider_email',
        'joining_date':'service_provider_joining_date'})


        #Aligning the columns as per the target table

        df_final = df_renamed[['service_provider_id','service_provider_name','service_provider_specialization','service_provider_phone','service_provider_email','service_provider_joining_date','created_at','updated_at']]
        df_final_count = len(df_final)

        print("Trasnformation process Complete")
    
    except Exception as e:
            print("Failed during Transformation",e)
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
    
    try:
        load_to_target(df_final,target_table)
        now = datetime.now(ist)
        log_etl_status(job_name, 'Completed', len(insert_df), len(update_df), now)
    except Exception as e:
        now = datetime.now(ist)
        log_etl_status(job_name, 'Failed', 0, 0, now)
        print(f"Error during Loading to Target table: {e}")
        raise