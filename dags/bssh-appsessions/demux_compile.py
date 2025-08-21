import os
import boto3
import requests
import pandas as pd
from io import StringIO
import io
from airflow.models import Variable, Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.appsession_transform import transform_appsession_data
from botocore.exceptions import ClientError
from utils.utils import silver_transform_to_db
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# ---- CONFIG ----
BUCKET_NAME = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "illumina_appsession_loader.sql"
AWS_CONN_ID = "aws"
DEMUX_METRICS_PREFIX = "bssh/intermediate/demux_metrics/"
PREFIX = "bssh/Demux/"
FILENAME_SUFFIX = "Demultiplex_Stats.csv"
YIELD_BUCKET = "bgsi-data-dwh-bronze"
YIELD_PREFIX = "illumina/qs/"
YIELD_FILENAME_SUFFIX = "Quality_Metrics.csv"
API_BASE_URL = "https://api.aps4.sh.basespace.illumina.com/v2/runs"
API_TOKEN = Variable.get("BSSH_APIKEY2")
def get_boto3_client_from_connection(conn_id='aws_default', service='s3'):
    conn = Connection.get_connection_from_secrets(conn_id)
    return boto3.client(
        service,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )
def clean_biosample_column(df, column="BioSampleName"):
    """Standardize biosample column for consistent merging."""
    df[column] = (
        df[column]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
    )
    return df

def load_yield_csv(bcl_df):
    try:
        s3 = get_boto3_client_from_connection(conn_id=AWS_CONN_ID)
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=YIELD_BUCKET, Prefix=YIELD_PREFIX)

        yield_dfs = []
        file_count = 0

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(YIELD_FILENAME_SUFFIX):
                    logger.info(f"📄 Loading Yield file: {key}")
                    file_count += 1
                    try:
                        body = s3.get_object(Bucket=YIELD_BUCKET, Key=key)["Body"]
                        df = pd.read_csv(StringIO(body.read().decode("utf-8")))

                        if "SampleID" not in df.columns or "Yield" not in df.columns:
                            logger.warning(f"⚠️ Skipping file (missing columns): {key}")
                            continue

                        df = df[df["SampleID"].astype(str).str.upper() != "UNDETERMINED"]
                        df["Yield"] = pd.to_numeric(df["Yield"], errors="coerce")
                        df = df[["SampleID", "Yield"]]
                        yield_dfs.append(df)
                    except Exception as e:
                        logger.error(f"❌ Failed to parse Yield CSV {key}: {e}")
        
        if not yield_dfs:
            logger.warning("🚫 No valid Yield files loaded.")
            return bcl_df

        logger.info(f"✅ Parsed {file_count} Quality_Metrics.csv files.")

        # Combine and aggregate
        all_yield_df = pd.concat(yield_dfs, ignore_index=True)
        all_yield_df["SampleID"] = all_yield_df["SampleID"].astype(str)
        agg_df = all_yield_df.groupby("SampleID", as_index=False)["Yield"].sum()
        agg_df.rename(columns={"SampleID": "BioSampleName"}, inplace=True)

        # CleanDataFrames
        agg_df = clean_biosample_column(agg_df, "BioSampleName")
        bcl_df = clean_biosample_column(bcl_df, "BioSampleName")

        logger.info("📊 Yield aggregation complete. Sample:")
        logger.info(agg_df.head(10).to_string(index=False))

        # Merge 
        merged = pd.merge(bcl_df, agg_df, on="BioSampleName", how="left")

        unmatched = merged[(merged["RowType"] == "BioSample") & (merged["Yield"].isna())]
        if not unmatched.empty:
            logger.warning(f"⚠️ {len(unmatched)} BioSample rows did not match any Yield entry.")
            logger.info(f"🕵️ Example unmatched BioSampleNames:\n{unmatched['BioSampleName'].drop_duplicates().head(10).to_list()}")

        return merged

    except Exception as e:
        logger.error(f"❌ Failed to load or merge Yield CSVs: {e}")
        return bcl_df
    
def transform_data(df, curr_ds):
    logger.info("ℹ️ No transformation applied in transform_data().")
    return df

def read_and_calculate_percentage_reads(**kwargs):
    ds = kwargs["ds"]

    s3 = get_boto3_client_from_connection(conn_id=AWS_CONN_ID)
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)

    matching_keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(FILENAME_SUFFIX):
                matching_keys.append(obj["Key"])

    if not matching_keys:
        logger.warning("🚫 No matching Demultiplex_Stats.csv files found.")
        return

    all_dfs = []
    for key in matching_keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        csv_content = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))
        df = df[df["SampleID"] != "Undetermined"]
        # coerce numeric for columns that start with '#' or '%'
        for col in df.columns:
            if col.startswith("#") or col.startswith("%"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        all_dfs.append(df)

    if not all_dfs:
        logger.warning("🚫 No usable demux data.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # ✅ sum by SampleID (the key)
    grouped_df = combined_df.groupby("SampleID", as_index=False).agg({
        "# Reads": "sum",
        "# Perfect Index Reads": "sum",
        "# One Mismatch Index Reads": "sum",
        "# Two Mismatch Index Reads": "sum",
    })

    # overall percent-of-total reads
    total_reads = grouped_df["# Reads"].sum()
    grouped_df["% Reads"] = (grouped_df["# Reads"] / total_reads * 100).round(2)

    # normalize for downstream merge (BioSampleName)
    grouped_df.rename(columns={"SampleID": "BioSampleName"}, inplace=True)
    grouped_df = clean_biosample_column(grouped_df, "BioSampleName")

    # write aggregated metrics back under the base prefix (no intermediate)
    out_key = f"{PREFIX}demux_metrics_{ds}.csv"
    s3.put_object(Bucket=BUCKET_NAME, Key=out_key, Body=grouped_df.to_csv(index=False).encode("utf-8"))
    logger.info(f"✅ Demux metrics (summed by SampleID) written to s3://{BUCKET_NAME}/{out_key}")
    
def fetch_bclconvert_and_dump(aws_conn_id, bucket_name, object_path, transform_func=None, **kwargs):
    curr_ds = kwargs['ds']

    #  Load latest BCL AppSession
    s3 = get_boto3_client_from_connection(conn_id=aws_conn_id)
    appsession_prefix = "bssh/appsessions/"
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=appsession_prefix)

    latest_obj = None
    for page in page_iterator:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv") and "bclconvert_appsessions" in obj["Key"]:
                if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                    latest_obj = obj

    if not latest_obj:
        logger.warning(" No BCLConvert AppSession file found.")
        return

    bcl_key = latest_obj["Key"]
    logger.info(f" Using BCLConvert AppSession file: {bcl_key}")
    obj = s3.get_object(Bucket=bucket_name, Key=bcl_key)
    bcl_df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

    bcl_df["RunId"] = bcl_df["RunId"].astype(str).str.strip().str.split(".").str[0]
    bcl_df = clean_biosample_column(bcl_df, "BioSampleName")

    # Add demux metrics
    # Add demux metrics (read from S3; no XCom)
    logger.info("📦 Loading Demultiplex metrics from S3...")
    ds = kwargs["ds"]
    demux_key = f"{PREFIX}demux_metrics_{ds}.csv"
    
    try:
        demux_obj = s3.get_object(Bucket=bucket_name, Key=demux_key)
        demux_df = pd.read_csv(StringIO(demux_obj["Body"].read().decode("utf-8")))
        demux_df = clean_biosample_column(demux_df, "BioSampleName")
        bcl_df = pd.merge(bcl_df, demux_df, on="BioSampleName", how="left")
        logger.info(f"✅ Demultiplex metrics merged from s3://{bucket_name}/{demux_key}")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchKey":
            logger.warning(f"⚠️ Demultiplex metrics not found at s3://{bucket_name}/{demux_key}")
        else:
            logger.error(f"❌ Failed to load demux metrics from S3: {e}")

    # Append Yield
    logger.info("🔗 Appending Yield data...")
    bcl_df = load_yield_csv(bcl_df)
    bcl_df.loc[bcl_df["RowType"] != "BioSample", "Yield"] = None

    # Fetch Total Flowcell Yield from API
    logger.info("🔗 Fetching Flowcell-level Yield totals...")
    bcl_df["TotalFlowcellYield"] = None
    run_rows = bcl_df[bcl_df["RowType"] == "Run"]

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
            logger.info(f"📡 Requesting TotalFlowcellYield for RunId={run_id}")
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            total_yield = data.get("YieldTotal")
    
            if total_yield is not None:
                bcl_df.loc[
                    (bcl_df["RowType"] == "Run") & (bcl_df["RunId"] == run_id),
                    "TotalFlowcellYield"
                ] = total_yield
                logger.info(f"✅ Assigned TotalFlowcellYield={total_yield} to RunId={run_id}")
            else:
                logger.warning(f"⚠️ No YieldTotal found for RunId={run_id}")
    
        except Exception as e:
            logger.error(f"❌ API error for RunId={run_id}: {e}")
    
    # After all runs have been processed, extract latest 200
    try:
        logger.info("📦 Filtering for the latest 200 Runs and BioSamples...")
        
        # Ensure DateCreated is datetime
        bcl_df["DateCreated"] = pd.to_datetime(bcl_df["DateCreated"], errors="coerce")

        # Show all rows/columns in logs
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 0)
        pd.set_option("display.max_colwidth", None)

        chunk_size = 24

        # ✅ Log latest 200 Run rows
        latest_runs = (
            bcl_df[bcl_df["RowType"] == "Run"]
            .sort_values("DateCreated", ascending=False)
            .head(200)
        )
        logger.info("📋 Final latest 200 Run rows (full preview):")
        for i in range(0, len(latest_runs), chunk_size):
            chunk = latest_runs.iloc[i:i+chunk_size]
            logger.info(f"\n🧾 Runs {i+1}–{i+len(chunk)}:\n{chunk.to_string(index=False)}")

        # ✅ Log latest 200 BioSample rows
        latest_samples = (
            bcl_df[bcl_df["RowType"] == "BioSample"]
            .sort_values("DateCreated", ascending=False)
            .head(200)
        )
        logger.info("📋 Final latest 200 BioSample rows (full preview):")
        for i in range(0, len(latest_samples), chunk_size):
            chunk = latest_samples.iloc[i:i+chunk_size]
            logger.info(f"\n🔬 BioSamples {i+1}–{i+len(chunk)}:\n{chunk.to_string(index=False)}")

    except Exception as e:
        logger.warning(f"⚠️ Failed to extract or print latest Run and BioSample rows: {e}")
    # ✅ Write final output CSV to S3 (bronze/final_output)
    try:
        out_df = transform_func(bcl_df.copy(), curr_ds) if transform_func else bcl_df
        out_key = f"{object_path}/bclconvert_appsessions_{curr_ds}.csv"
        s3.put_object(Bucket=bucket_name, Key=out_key, Body=out_df.to_csv(index=False).encode("utf-8"))
        logger.info(f"✅ Wrote final output to s3://{bucket_name}/{out_key}")
    except Exception as e:
        logger.error(f"❌ Failed to write final output to S3: {e}")
        raise
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
BASE_DIR = os.path.dirname(__file__)
loader_path = os.path.join(BASE_DIR, "include", "loader", LOADER_QEURY)
with open(loader_path) as f:
    loader_query = f.read()

process_task = PythonOperator(
    task_id='process_demux_csvs',
    python_callable=read_and_calculate_percentage_reads,
    dag=dag,
    provide_context=True
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

silver_upsert_appsessions = PythonOperator(
    task_id="silver_upsert_bclconvert_appsessions",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": BUCKET_NAME,
        "object_path": "bssh/final_output",        # read from your dumped final output
        "transform_func": transform_appsession_data,  # <-- use the appsession transform here
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}",
    },
    templates_dict={"insert_query": loader_query},   # <-- illumina_appsession_loader.sql
    provide_context=True,
)
process_task >> fetch_and_dump_task >> silver_upsert_appsessions
