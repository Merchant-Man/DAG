import boto3
import pandas as pd
from io import StringIO
import io
from airflow.models import Variable, Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# ---- CONFIG ----
BUCKET_NAME = Variable.get("S3_DWH_BRONZE")
AWS_CONN_ID = "aws"
PREFIX = "bssh/Demux/"                 
FILENAME_SUFFIX = "Demultiplex_Stats.csv"
YIELD_BUCKET = "bgsi-data-dwh-bronze"
YIELD_PREFIX = "illumina/qs/"
YIELD_FILENAME_SUFFIX = "Quality_Metrics.csv"
API_BASE_URL = "https://api.aps4.sh.basespace.illumina.com/v2/runs"
API_TOKEN = Variable.get("BSSH_APIKEY1")
def get_boto3_client_from_connection(conn_id='aws_default', service='s3'):
    conn = Connection.get_connection_from_secrets(conn_id)
    return boto3.client(
        service,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )

def transform_data(df, curr_ds):
    logger.info("No transformation applied.")
    return df
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
    # Initialize column
    if "TotalFlowcellYield" not in merged_df.columns:
        merged_df["TotalFlowcellYield"] = None
    
    run_rows = merged_df[merged_df["RowType"] == "Run"]
    
    for _, row in run_rows.iterrows():
        run_id = row.get("RunId")
        if not run_id or run_id.lower() == "nan":
            continue
    
        api_url = f"{API_BASE_URL}/{run_id}/sequencingstats"
        headers = {
            "x-access-token": API_TOKEN,
            "Accept": "application/json"
        }
    
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            total_yield = data.get("YieldTotal")
    
            if total_yield is not None:
                mask = (merged_df["RowType"] == "Run") & (merged_df["RunId"] == run_id)
                merged_df.loc[mask, "TotalFlowcellYield"] = total_yield
                logger.info(f"✅ Assigned TotalFlowcellYield={total_yield} to RunId={run_id}")
            else:
                logger.warning(f"No yield found for RunId={run_id}")
        except Exception as e:
            logger.error(f"API call failed for RunId={run_id}: {e}")
    yield_s3 = get_boto3_client_from_connection(conn_id=AWS_CONN_ID)
    yield_paginator = yield_s3.get_paginator("list_objects_v2")
    yield_pages = yield_paginator.paginate(Bucket=YIELD_BUCKET, Prefix=YIELD_PREFIX)
    
    latest_yield_obj = None
    quality_keys = []
    for page in yield_pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            print(obj["Key"])
            if key.endswith(YIELD_FILENAME_SUFFIX) and "/fil." in key:
                quality_keys.append(key)
                if latest_yield_obj is None or obj["LastModified"] > latest_yield_obj["LastModified"]:
                    latest_yield_obj = obj
    if not quality_keys:
        logger.warning("No Quality_Metrics.csv files found under illumina/qs/fil.*/")
        return
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
    return merged_df

def fetch_bclconvert_and_dump(aws_conn_id, bucket_name, object_path, transform_func=None, **context):
    curr_ds = datetime.today().strftime('%Y-%m-%d')
    merged_df = read_and_calculate_percentage_reads()
    df = merged_df.copy()
    df = transform_func(merged_df, curr_ds) if transform_func else merged_df.copy()
    #timestamps
    df["created_at"] = curr_ds
    df["updated_at"] = curr_ds
    # Convert DataFrame to CSV
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    # Upload to S3
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3_path = f"{object_path}/{curr_ds}/bclconvertandQC-{curr_ds}.csv"
    s3.load_string(buffer.getvalue(), key=s3_path, bucket_name=bucket_name, replace=True)
    logger.info(f"✅ Saved to S3: s3://{bucket_name}/{s3_path}")

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

fetch_and_dump_task = PythonOperator(
    task_id="bronze_fetch_bssh_bclconvertandQC",
    python_callable=fetch_bclconvert_and_dump,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": BUCKET_NAME,
        "object_path": "bssh/final_output",
        "transform_func": transform_data  
    },
    provide_context=True
)
process_task >> fetch_and_dump_task
