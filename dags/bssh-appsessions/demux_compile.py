import boto3
import pandas as pd
from io import StringIO
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.models import Connection
# ---- CONFIG ----
BUCKET_NAME = Variable.get("S3_DWH_BRONZE")       
PREFIX = "bssh/Demux/"                 
FILENAME_SUFFIX = "Demultiplex_Stats.csv"  
def get_boto3_client_from_connection(conn_id='aws_default', service='s3'):
    conn = connection.Connection.get_connection_from_secrets(aws)
    return boto3.client(
        service,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

def read_and_calculate_percentage_reads():
    s3 = get_boto3_client_from_connection()
    bucket = Variable.get("S3_DWH_BRONZE")
    prefix = "bssh/Demux/"
    suffix = "Demultiplex_Stats.csv"

    # Collect all matching files
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    matching_keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                matching_keys.append(obj["Key"])

    all_dfs = []
    for key in matching_keys:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(content))
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
        '# Two Mismatch Index Reads': 'sum'
    })

    # Total reads across all samples
    total_reads = grouped_df['# Reads'].sum()

    # Recalculate % Reads
    grouped_df['% Reads'] = grouped_df['# Reads'] / total_reads * 100

    # Optional: round for display
    grouped_df['% Reads'] = grouped_df['% Reads'].round(4)

    print(grouped_df[['SampleID', '# Reads', '% Reads']])
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
    python_callable=process_demux_files,
    dag=dag
)
