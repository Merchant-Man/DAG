import boto3
import pandas as pd
from io import StringIO
from airflow.models import Variable, Connection
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# ---- CONFIG ----
BUCKET_NAME = Variable.get("S3_DWH_BRONZE")
AWS_CONN_ID = "aws"
PREFIX = "bssh/Demux/"                 
FILENAME_SUFFIX = "Demultiplex_Stats.csv"  
def get_boto3_client_from_connection(conn_id='aws_default', service='s3'):
    conn = Connection.get_connection_from_secrets(conn_id)
    return boto3.client(
        service,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )
def process_demux_files():
    return read_and_calculate_percentage_reads()
    
def read_and_calculate_percentage_reads():
    s3 = get_boto3_client_from_connection(conn_id=AWS_CONN_ID)
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)

    matching_keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(FILENAME_SUFFIX):
                matching_keys.append(obj["Key"])

    if not matching_keys:
        print("No matching Demultiplex_Stats.csv files found.")
        return

    all_dfs = []
    for key in matching_keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        csv_content = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        df = df[df["SampleID"] != "Undetermined"]
        for col in df.columns:
            if col.startswith("#") or col.startswith("%"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        all_dfs.append(df)
    if not all_dfs:
        print("No data found.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Group and sum
    grouped_df = combined_df.groupby("SampleID", as_index=False).agg({
        '# Reads': 'sum',
        '# Perfect Index Reads': 'sum',
        '# One Mismatch Index Reads': 'sum',
        '# Two Mismatch Index Reads': 'sum',
        '% Reads': 'sum'
    })

    print(grouped_df[['SampleID', '# Reads', '% Reads']])
    print(grouped_df.columns.tolist())
    return grouped_df

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
    python_callable=read_and_calculate_percentage_reads,  
    dag=dag
)
