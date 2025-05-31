from framework.ETL_Functions import *
import pandas as pd
from datetime import datetime, date
import pytz
import pymysql
from db_connections import *


ist = pytz.timezone('Asia/Kolkata')

def run_facility_details(job_name, source_table, target_table, stage_table, primary_key):


    now = datetime.now(ist)

    # Getting timestamp of last succesfull job run

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


    # Filter only unique records for dim

    df_facility = df_source['facility_name'].drop_duplicates()
    #Loading to Stage table

    try:
        load_to_stage(df_facility, stage_table)
    except Exception as e:
        print('Failed to loading data to Stage DB',e)
        raise

    try:
    # Read data from target table
        dim_facility_details_df = get_dim_data(dim_table = 'dim_facility_details')
    except Exception as e:
        print('Failed to read data from target table',e)
        raise   

    try:
        print('Transformation process begins')
        # Filtering the records present in source but not in target.

        facilities_target = list(dim_facility_details_df['facility_name'])
        facilities_source = list(df_source['facility_name'].unique())

        new_facilities = [facility_name for facility_name in facilities_source if facility_name not in facilities_target]
        #Generating Id for new records

        current_max_id = len(facilities_target)
        #new_ids = pd.Series(range(current_max_id + 1, current_max_id + 1 + len(new_facilities)))
        new_ids = pd.Series(range(current_max_id + 1, current_max_id + 1 + len(new_facilities)), dtype=int)


        df_final = pd.DataFrame({'facility_dim_id': new_ids,
                                    'clinic_source_id':[facility[:3].upper() + str('_') + str(id) for facility, id in zip(new_facilities, new_ids)],
                                    'facility_name': new_facilities,
                                    'created_at':now })
        print('Transformation process complete')
    except Exception as e:
        print('Error during Transformation process',e)
        raise      

    #Loading to target table

    try:
        load_to_target(df_final,target_table)
        log_etl_status(job_name, 'Completed', len(df_final), 0, now)
    except Exception as e:
        print(f"Error during Loading to Target table: {e}")
        log_etl_status(job_name, 'Failed', 0, 0, now)
        raise