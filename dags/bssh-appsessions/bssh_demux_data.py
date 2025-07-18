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
API_KEY = Variable.get("API_KEY_ICA")
PROJECT_ID = Variable.get("ICA_Project_id")
BASE_URL = "https://ica.illumina.com/ica/rest/api"
AWS_CONN_ID = "aws"
BSSH_CONN_ID = "bssh"
BSSH_APIKEY = Variable.get("BSSH_APIKEY1")
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
# Updated OBJECT_PATH to match what silver_transform_to_db expects
OBJECT_PATH = "bssh/Demux"
OBJECT_PATH2 = "

def create_download_url(api_key: str, project_id: str, file_id: str) -> str:
    url = f"{BASE_URL}/projects/{project_id}/data/{file_id}:createDownloadUrl"
    headers = {
        "accept": "application/vnd.illumina.v3+json",
        "X-API-Key": api_key
    }
    response = requests.post(url, headers=headers, data='')
    response.raise_for_status()
    return response.json().get("url")

def fetch_bclconvertDemux_and_dump(aws_conn_id, bucket_name, object_path_prefix,
                                   transform_func=None, curr_ds=None, **kwargs):
    analyses = []
    page_size = 100
    page_offset = 0
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    logger = LoggingMixin().log
    curr_ds = kwargs["ds"]
    curr_date_start = datetime.strptime(curr_ds, "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=1)
    curr_date_end = curr_date_start + timedelta(days=2)

    HEADERS = {
        "accept": "application/vnd.illumina.v3+json",
        "X-API-Key": API_KEY
    }

    logger.info(f"Fetching sessions for: {curr_ds}")

    while True:
        url = (
            f"{BASE_URL}/projects/{PROJECT_ID}/analyses"
            f"?pageSize={page_size}&pageOffset={page_offset}&sort=reference%20desc"
        )
        logger.info(f"Requesting URL: {url}")
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        analyses.extend(items)

        logger.info(f"Fetched {len(items)} analyses (offset {page_offset})")

        if len(items) < page_size:
            break
        page_offset += page_size

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

            # --- Handle Demultiplex_Stats.csv ---
            demux_file_path = f"/ilmn-analyses/{reference}/output/Reports/Demultiplex_Stats.csv"
            demux_encoded = urllib.parse.quote(demux_file_path)

            demux_query = (
                f"{BASE_URL}/projects/{PROJECT_ID}/data"
                f"?filePath={demux_encoded}"
                f"&filenameMatchMode=EXACT"
                f"&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
                f"&status=AVAILABLE&type=FILE"
            )

            demux_response = requests.get(demux_query, headers=HEADERS)
            demux_response.raise_for_status()
            demux_items = demux_response.json().get("items", [])

            if demux_items:
                file_id = demux_items[0]["data"]["id"]
                download_url = create_download_url(API_KEY, PROJECT_ID, file_id)
                response = requests.get(download_url)
                response.raise_for_status()

                s3_key = f"{object_path_prefix}/{reference}/{lp_ref}_Demultiplex_Stats.csv"
                s3.load_bytes(
                    bytes_data=response.content,
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=True
                )
                logger.info(f"Uploaded Demultiplex_Stats to: s3://{bucket_name}/{s3_key}")
            else:
                logger.info(f"Demultiplex_Stats.csv not found for {reference}")

            # Quality_Metrics.csv
            quality_file_path = f"/ilmn-analyses/{reference}/output/Reports/Quality_Metrics.csv"
            quality_encoded = urllib.parse.quote(quality_file_path)
            
            quality_query = (
                f"{BASE_URL}/projects/{PROJECT_ID}/data"
                f"?filePath={quality_encoded}"
                f"&filenameMatchMode=EXACT"
                f"&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
                f"&status=AVAILABLE&type=FILE"
            )
            
            quality_response = requests.get(quality_query, headers=HEADERS)
            quality_response.raise_for_status()
            quality_items = quality_response.json().get("items", [])
            
            if quality_items:
                file_id = quality_items[0]["data"]["id"]
                download_url = create_download_url(API_KEY, PROJECT_ID, file_id)
                response = requests.get(download_url)
                response.raise_for_status()
            
                # Final S3 key format: illumina/qs/{file_id}/Quality_Metrics.csv
                qs_s3_key = f"illumina/qs/{file_id}/Quality_Metrics.csv"
                s3.load_bytes(
                    bytes_data=response.content,
                    key=qs_s3_key,
                    bucket_name="bgsi-data-dwh-bronze",
                    replace=True
                )
                logger.info(f"Uploaded Quality_Metrics to: s3://bgsi-data-dwh-bronze/{qs_s3_key}")
            else:
                logger.info(f"Quality_Metrics.csv not found for {reference}")

        except Exception as e:
            logger.error(f"Error processing analysis {reference}: {e}", exc_info=True)
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
