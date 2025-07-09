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
YIELD_BUCKET = "bgsi-data-dwh-bronze"
YIELD_PREFIX = "illumina/qs/"
YIELD_FILENAME_SUFFIX = "Quality_Metrics.csv"
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

    # Append
    appsession_prefix = "bssh/appsessions/"
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=appsession_prefix)

    latest_obj = None
    for page in page_iterator:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv") and "bclconvert_appsessions" in obj["Key"]:
                if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                    latest_obj = obj

    if not latest_obj:
        print("No BCLConvert AppSession CSV found.")
        return

    bcl_key = latest_obj["Key"]
    print(f" Using latest AppSession file: {bcl_key}")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=bcl_key)
    bcl_df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

    # Merge on BioSampleName
    merged_df = pd.merge(
        bcl_df,
        grouped_df.rename(columns={"SampleID": "BioSampleName"}),
        on="BioSampleName",
        how="left"
    )
    yield_s3 = get_boto3_client_from_connection(conn_id=AWS_CONN_ID)
    yield_paginator = yield_s3.get_paginator("list_objects_v2")
    yield_pages = yield_paginator.paginate(Bucket=YIELD_BUCKET, Prefix=YIELD_PREFIX)
    
    latest_yield_obj = None
    for page in yield_pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(YIELD_FILENAME_SUFFIX):
                if latest_yield_obj is None or obj["LastModified"] > latest_yield_obj["LastModified"]:
                    latest_yield_obj = obj
        logger.warning("No Yield CSV found.")
        return
    yield_key = latest_yield_obj["Key"]
    logger.info(f"Using Yield file: {yield_key}")
    obj = yield_s3.get_object(Bucket=YIELD_BUCKET, Key=yield_key)
    yield_df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
    if "SampleID" not in yield_df.columns:
        logger.warning("Yield file missing 'BioSampleName' column.")
        return
    if "Yield" not in yield_df.columns:
        logger.warning("Yield file missing 'Yield' column.")
        return
    # Clean Yield column(Numerical)
    
    yield_df = yield_df[yield_df["SampleID"] != "Undetermined"]
    yield_df["Yield"] = pd.to_numeric(yield_df["Yield"], errors="coerce")
    
    # Aggregate Yield per SampleID, # Merge with main
    agg_yield_df = yield_df.groupby("SampleID", as_index=False)["Yield"].sum()
    agg_yield_df.rename(columns={"SampleID": "BioSampleName"}, inplace=True)
    
    # Merge into maindf
    merged_df = pd.merge(
        merged_df,
        agg_yield_df,
        on="BioSampleName",
        how="left"
    )
    logger.info("Merged Yield column into final DataFrame.")
    print(" Merged DataFrame:")
    print(merged_df.head())
    pd.set_option('display.max_rows', None)       # Show all rows
    pd.set_option('display.max_columns', None)    # Show all columns
    pd.set_option('display.width', None)          # Don't wrap lines
    pd.set_option('display.max_colwidth', None)   # Show full column contents

    print(merged_df)

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
