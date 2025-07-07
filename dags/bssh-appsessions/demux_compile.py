import boto3
import pandas as pd
from io import StringIO
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
# ---- CONFIG ----
BUCKET_NAME = Variable.get("S3_DWH_BRONZE")       
PREFIX = "bssh/Demux/"                 
FILENAME_SUFFIX = "Demultiplex_Stats.csv"  

def get_boto3_client_from_connection(conn_id='aws_default', service='s3'):
    conn = BaseHook.get_connection(conn_id)
    return boto3.client(
        service,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name='us-east-1'  # change if needed
    )
s3 = boto3.client("s3")

response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
matching_keys = [
    obj['Key']
    for obj in response.get('Contents', [])
    if obj['Key'].endswith(FILENAME_SUFFIX)
]
all_dfs = []

for key in matching_keys:
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    csv_content = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))

    # Skip 'Undetermined' rows
    df = df[df['SampleID'] != 'Undetermined']

    # Convert numeric columns
    for col in df.columns:
        if col.startswith('#') or col.startswith('%'):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    all_dfs.append(df)
combined_df = pd.concat(all_dfs, ignore_index=True)

grouped_df = df.groupby('SampleID', as_index=False).sum(numeric_only=True)
index_map = df.groupby('SampleID')['Index'].first().reset_index()
final_df = pd.merge(index_map, grouped_df, on='SampleID')
print(final_df)

# ----------------------------
# DAG Definition
# ----------------------------

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'bssh_Demux_Compile',
    default_args=default_args,
    description='Fetch Demux_Compile QC load to S3 + RDS',
    schedule_interval=timedelta(days=1),
    catchup=False
)

process_task = PythonOperator(
    task_id='process_demux_csvs',
    python_callable=process_demux_files,
    dag=dag
)
