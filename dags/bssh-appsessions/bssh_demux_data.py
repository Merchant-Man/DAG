from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import re
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dateutil.parser import isoparse
from datetime import timezone
import urllib.parse
# Silver task
from utils.utils import fetch_and_dump, silver_transform_to_db

# --- Configuration ---
API_KEY = "04LlMKg4K0asFGREmIXhucZ3IV2Hinx"
PROJECT_ID = "7feb6619-714b-48f7-a7fd-75ad264f9c55"
BASE_URL = "https://ica.illumina.com/ica/rest/api"
AWS_CONN_ID = "aws"
BSSH_CONN_ID = "bssh"
BSSH_APIKEY = Variable.get("BSSH_APIKEY1")
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
# Updated OBJECT_PATH to match what silver_transform_to_db expects
OBJECT_PATH = "bssh/Demux"

def fetch_bclconvertDemux_and_dump(aws_conn_id, bucket_name, object_path_prefix,
                                   transform_func=None, curr_ds=None, **kwargs):
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    logger = LoggingMixin().log
    curr_ds = kwargs["ds"]
    curr_date_start = datetime.strptime(curr_ds, "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=1)
    curr_date_end = curr_date_start + timedelta(days=2)

    HEADERS = {
        "accept": "application/vnd.illumina.v3+json",
        "X-API-Key": API_KEY
    }

    logger.info(f"Fetching sessions for: {curr_ds}")

    resp = requests.get(f"{BASE_URL}/projects/{PROJECT_ID}/analyses", headers=HEADERS)
    resp.raise_for_status()
    analyses = resp.json().get("items", [])

    if not analyses:
        logger.info("No analyses found.")
        return

    # Sort by timeCreated (latest first)
    latest_analyses = sorted(analyses, key=lambda a: a["timeCreated"], reverse=True)

    def extract_lp_reference(reference_str):
        match = re.search(r"(LP[-_]?\d{7}(?:-P\d)?(?:[-_](?:rerun|redo))?)", reference_str, re.IGNORECASE)
        return match.group(1) if match else None

    for analysis in latest_analyses:
        try:
            reference = analysis.get("reference")
            logger.info(f"Checking analysis reference: {reference}")
            if not reference:
                continue

            lp_ref = extract_lp_reference(reference)
            if not lp_ref:
                logger.warning(f"Could not extract LP reference from: {reference}")
                continue

            file_path = f"/ilmn-analyses/{reference}/output/Reports/Demultiplex_Stats.csv"
            encoded_path = urllib.parse.quote(file_path)

            file_query_url = (
                f"{BASE_URL}/projects/{PROJECT_ID}/data"
                f"?filePath={encoded_path}"
                f"&filenameMatchMode=EXACT"
                f"&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
                f"&status=AVAILABLE&type=FILE"
            )

            file_response = requests.get(file_query_url, headers=HEADERS)
            file_response.raise_for_status()
            file_items = file_response.json().get("items", [])

            if not file_items:
                logger.info(f"Demultiplex_Stats.csv not found for {reference}")
                continue

            file_id = file_items[0]["data"]["id"]
            logger.info(f"Found file with ID: {file_id}")

            def create_download_url(api_key: str, project_id: str, file_id: str) -> str:
                url = f"{BASE_URL}/projects/{project_id}/data/{file_id}:createDownloadUrl"
                headers = {
                    "accept": "application/vnd.illumina.v3+json",
                    "X-API-Key": api_key
                }
                response = requests.post(url, headers=headers, data='')
                response.raise_for_status()
                result = response.json()
                return result.get("url")

            download_url = create_download_url(API_KEY, PROJECT_ID, file_id)
            logger.info(f"Download URL: {download_url}")

            response = requests.get(download_url)
            response.raise_for_status()

            s3_key = f"{object_path_prefix}/{reference}/{lp_ref}_Demultiplex_Stats.csv"
            csv_buffer = io.BytesIO(response.content)

            s3.load_bytes(
                bytes_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )

            logger.info(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")

        except Exception as e:
            logger.error(f"Error processing analysis {reference}: {str(e)}", exc_info=True)
            continue

# ----------------------------
# DAG Definition
# ----------------------------

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'bssh_demuxQC',
    default_args=default_args,
    description='Fetch BCLConvert Demux QC from BSSH and load to S3 + RDS',
    schedule_interval=timedelta(days=1),
    catchup=False
)
fetch_demux_to_s3 = PythonOperator(
    task_id='fetch_bclconvert_demux_qc',
    python_callable=fetch_bclconvertDemux_and_dump,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path_prefix": OBJECT_PATH,
        "transform_func": None
    },
    provide_context=True,
    dag=dag
)

fetch_demux_to_s3
