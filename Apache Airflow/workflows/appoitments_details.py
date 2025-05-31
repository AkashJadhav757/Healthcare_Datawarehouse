from framework.ETL_Functions import *
import pandas as pd
from datetime import datetime, date
import pytz
import pymysql
from db_connections import *


ist = pytz.timezone('Asia/Kolkata')

def run_appointment_details(job_name, source_table, target_table, stage_table, primary_key):
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

    # Step 1: Cast datetimes
    df_source = df_source.astype({
        'created_at': 'datetime64[ns]',
        'updated_at': 'datetime64[ns]'
    })

    # Step 2: Convert timedelta string like "0 days 10:00:00" to datetime.time
    df_source['appointment_time'] = pd.to_timedelta(df_source['appointment_time'], errors='coerce')
    df_source['appointment_time'] = df_source['appointment_time'].apply(
        lambda x: (pd.Timestamp('1900-01-01') + x).time() if pd.notnull(x) else None
    )

    try:
        load_to_stage(df_source, stage_table)
    except Exception as e:
        print('Failed to loading data to Stage DB',e)
        raise
    try:
        print('Reading data from Dimensions')
        
        dim_patient_details_df =get_dim_data(dim_table = 'dim_patient_details')
        dim_service_provider_df = get_dim_data(dim_table = 'dim_service_provider')
        dim_facility_details_df = get_dim_data(dim_table = 'dim_facility_details')
        dim_insurance_details_df = get_dim_data(dim_table = 'dim_insurance_firms')
        invoice_details_df = get_dim_data(dim_table = 'invoice_details')

    except Exception as e:
        print('Failed to read data from Dimensions',e)
        raise
    try:
        print(" Joining Fact table with Dimension tables")

        df_source = pd.merge(df_source, dim_patient_details_df, how='left', on='patient_id',suffixes=('', '_patient'))
        df_source = pd.merge(df_source, dim_service_provider_df, how='left', on='service_provider_name',suffixes=('', '_provider'))
        df_source = pd.merge(df_source, dim_facility_details_df, how='left', on='facility_name',suffixes=('', '_facility'))
        df_source = pd.merge(df_source, invoice_details_df, how='left', on='appointment_id',suffixes=('', '_invoice'))
        df_source = pd.merge(df_source, dim_insurance_details_df, how='left', left_on='insurance_company',right_on ='ins_firm_name',suffixes=('', '_invoice'))

        print("Succesfully Joined Fact table with Dimesion tables")
    except Exception as e:
        print('Error occured while joining Fact table with Dimension tables',e)
        raise
    try:
        df_final = df_source[['appointment_id','patient_id','service_provider_id','invoice_id','total_amount','payment_status','appointment_date','appointment_status','appointment_type','ins_firm_code',
            'clinic_source_id','reason_for_visit','icd_code', 'cpt_code','created_at', 'updated_at']]
    except Exception as e:
        print('Error occured while selecting columns',e)
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
        load_to_target(df_final,target_table)
        log_etl_status(job_name, 'Completed', len(insert_df), len(update_df), now)
    except Exception as e:
        print(f"Error during Loading to Target table: {e}")
        log_etl_status(job_name, 'Failed', 0, 0, now)
        raise
