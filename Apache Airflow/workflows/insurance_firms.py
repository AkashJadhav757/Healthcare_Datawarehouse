from framework.ETL_Functions import *
import pandas as pd
from datetime import datetime, date
import pytz
import pymysql
from db_connections import *


ist = pytz.timezone('Asia/Kolkata')


def run_insurance_firms(job_name, source_table, target_table, stage_table):

    now = datetime.now(ist)
    try:
        last_run = get_last_etl_run_timestamp(job_name)
    except Exception as e:
        print('Failed to get last etl run details',e)
        raise

    try:
        df_source = extract_new_data(source_table, last_run)
        df_insurance = df_source['insurance_company'].drop_duplicates()
    except Exception as e:
        print('Failed to get last etl run details',e)
        raise

     #Loading to Stage table
    try:
        load_to_stage(df_insurance, stage_table)
    except Exception as e:
        print('Failed to loading data to Stage DB',e)
        raise

    # Read data from target table
    try:
        dim_insurance_firms_df = get_dim_data(dim_table = 'dim_insurance_firms')
    except Exception as e:
        print('Error reading Target table',e)
        raise
    
    # Filtering the records present in source but not in target.
    try:
        print('Filtering the records present in source but not in target')
        

        df_insurance_target = list(dim_insurance_firms_df['ins_firm_name'])
        df_insurance_source = list(df_source['insurance_company'].unique())

        new_ins_firms = [ins_firm for ins_firm in df_insurance_source if ins_firm not in df_insurance_target]
        print('Filter process complete')
    except Exception as e:
        print('Error during filtering records',e)
        raise

    #Generating Id for new records
    try:
        print('Generating Id for new records')

        current_max_id = len(df_insurance_target)

        new_ids = pd.Series(range(current_max_id + 1, current_max_id + 1 + len(new_ins_firms)), dtype=int)


        df_final = pd.DataFrame({'ins_firm_dim_id': new_ids,
                                    'ins_firm_code':[ins_firm[:3].upper() + str('_') + str(id) for ins_firm, id in zip(new_ins_firms, new_ids)],
                                    'ins_firm_name': new_ins_firms,
                                    'created_at':now })
        print('ID generating process complete')
    except Exception as e:
        print('Error Generating Id for new records',e)
        raise


    #Loading to target table
    try:
        load_to_target(df_final,target_table)
        log_etl_status(job_name, 'Completed', len(df_final), 0, now)
    except Exception as e:
        print(f"Error during Loading to Target table: {e}")
        log_etl_status(job_name, 'Failed', 0, 0, now)
        raise