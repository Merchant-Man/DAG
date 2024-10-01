from datetime import datetime, timedelta
import pandas as pd
import io
import ast

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER=Variable.get("S3_DWH_SILVER")
prefix                          = "simbiox/biosamples/"
tbl_data_biosample_prefix       = "simbiox/tbl_data_biosample/"
tbl_master_biobank_key          = "simbiox/tbl_master_biobank/latest.csv"
tbl_master_sample_types_key     = "simbiox/tbl_master_sample_types/latest.csv"
tbl_master_specimen_types_key   = "simbiox/tbl_master_specimen_types/latest.csv"
tbl_master_status_key           = "simbiox/tbl_master_status/latest.csv"

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver-simbiox-biosample-simple',
    default_args=default_args,
    description='ETL pipeline to merge CSV files from S3',
    schedule_interval=timedelta(days=5),
)

def etl_to_s3(**kwargs):
    s3 = S3Hook(aws_conn_id='aws')
    
    # List all objects in the S3 prefix
    files = s3.list_keys(bucket_name=S3_DWH_BRONZE, prefix=tbl_data_biosample_prefix)
    
    if not files:
        raise ValueError(f"No files found in {tbl_data_biosample_prefix}")
    
    all_data_frames = []

    for file_key in files:
        if file_key.endswith('.csv'):
            # Read each CSV file into a DataFrame
            csv_obj = s3.get_key(key=file_key, bucket_name=S3_DWH_BRONZE)
            df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
            all_data_frames.append(df)

    # Merge all DataFrames into one
    df = pd.concat(all_data_frames, ignore_index=True)

    tbl_master_biobank          = pd.read_csv(io.BytesIO(s3.get_key(key=tbl_master_biobank_key, bucket_name=S3_DWH_SILVER).get()['Body'].read()))
    tbl_master_sample_types     = pd.read_csv(io.BytesIO(s3.get_key(key=tbl_master_sample_types_key, bucket_name=S3_DWH_SILVER).get()['Body'].read()))
    tbl_master_specimen_types   = pd.read_csv(io.BytesIO(s3.get_key(key=tbl_master_specimen_types_key, bucket_name=S3_DWH_SILVER).get()['Body'].read()))
    tbl_master_status           = pd.read_csv(io.BytesIO(s3.get_key(key=tbl_master_status_key, bucket_name=S3_DWH_SILVER).get()['Body'].read()))

    # Remove duplicates from the main DataFrame
    df = df.drop_duplicates()

    df['id_patient']             = df['patient_id'].astype(str)
    df['code_repository']        = df['repository_code'].astype(str)
    df['code_box']               = df['box_code'].astype(str)
    df['code_position']          = df['position_code'].astype(str)
    df['date_received']          = df['received_date'].astype(str)
    df['date_enumerated']        = df['enter_date_in_biorepo'].astype(str)
    df['origin_biobank']         = df['id_biobank'].astype(str)
    df['origin_code_repository'] = df['sample_old_code'].astype(str)
    df['origin_code_box']        = df['sample_box_old_code'].astype(str)
    df['biosample_type']         = df['sample_type_id'].astype(str)
    df['biosample_specimen']     = df['specimen_type_id'].astype(str)
    df['biosample_volume']       = df['vol_product'].astype('int64')
    df['biosample_status']       = df['status'].astype(str)
    
    df = pd.merge(df, tbl_master_biobank[['biobank_id', 'biobank_nama']], left_on='origin_biobank', right_on='biobank_id', suffixes=('', '_biobank'))
    df['origin_biobank'] = df['biobank_nama'].astype(str)

    tbl_master_sample_types['id'] = tbl_master_sample_types['id'].astype(str)
    df = pd.merge(df, tbl_master_sample_types[['id', 'sample_name']], left_on='biosample_type', right_on='id', suffixes=('', '_sample_type'))
    df['biosample_type'] = df['sample_name'].astype(str)
    df.drop('id_sample_type', axis=1, inplace=True)

    tbl_master_specimen_types['id'] = tbl_master_specimen_types['id'].astype(str)
    df = pd.merge(df, tbl_master_specimen_types[['id', 'specimen_type_name']], left_on='biosample_specimen', right_on='id', suffixes=('', '_specimen_type'))
    df['biosample_specimen'] = df['specimen_type_name']
    df.drop('id_specimen_type', axis=1, inplace=True)

    tbl_master_status['id'] = tbl_master_status['id'].astype(str)
    df = pd.merge(df, tbl_master_status[['id', 'status_name']], left_on='biosample_status', right_on='id', suffixes=('', '_status'))
    df['biosample_status'] = df['status_name']
    df.drop('id_status', axis=1, inplace=True)

    df = df[['id_patient',
        'code_repository',
        'code_box',
        'code_position',
        'date_received',
        'date_enumerated',
        'origin_biobank',
        'origin_code_repository',
        'origin_code_box',
        'biosample_type',
        'biosample_specimen',
        'biosample_volume',
        'biosample_status']]

    # Convert cleaned DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    cleaned_data = csv_buffer.getvalue()

    # Use data_interval_start for timestamp
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'{prefix}{data_interval_start.isoformat()}.csv'

    # Use S3Hook to upload the cleaned CSV to S3
    s3 = S3Hook(aws_conn_id='aws')

    # Upload the file with timestamp in the name
    s3.load_string(
        string_data=cleaned_data,
        key=s3_key,
        bucket_name=S3_DWH_SILVER,
        replace=True
    )

    # Upload the same file as 'latest.csv'
    s3_key_latest = f'{prefix}latest.csv'
    s3.load_string(
        string_data=cleaned_data,
        key=s3_key_latest,
        bucket_name=S3_DWH_SILVER,
        replace=True
    )


etl_to_s3_task = PythonOperator(
    task_id='etl_to_s3',
    python_callable=etl_to_s3,
    provide_context=True,  # To pass kwargs
    dag=dag,
)

etl_to_s3_task
