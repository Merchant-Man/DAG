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
# Silver task
from utils.utils import fetch_and_dump, silver_transform_to_db

# --- Configuration ---
API_KEY = "04LlMKg4K0asFGREmIXhucZ3IV2Hinx"
PROJECT_ID = "7feb6619-714b-48f7-a7fd-75ad264f9c55"
BASE_URL = "https://ica.illumina.com/ica/rest/api"

HEADERS = {
    "accept": "application/vnd.illumina.v3+json",
    "X-API-Key": API_KEY
}

analyses_url = f"{BASE_URL}/projects/{PROJECT_ID}/analyses"
response = requests.get(analyses_url, headers=HEADERS)
response.raise_for_status()
analyses = response.json().get("items", [])

if not analyses:
    print(" No analyses found.")
    exit(1)

# latest analysis
latest_analysis = sorted(analyses, key=lambda a: a["timeCreated"], reverse=True)

for analysis in latest_analysis:
    reference = analysis.get("reference")
    print(f" Latest analysis reference: {reference}")
    if not reference: 
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
        print(" Demultiplex_Stats.csv not found.")
        exit(1)

    file_id = file_items[0]["data"]["id"]
    print(f" File ID: {file_id}")

    # download 
    def create_download_url(api_key: str, project_id: str, file_id: str) -> str:
        url = f"{BASE_URL}/projects/{project_id}/data/{file_id}:createDownloadUrl"
        headers = {
            "accept": "application/vnd.illumina.v3+json",
            "X-API-Key": api_key
        }
        response = requests.post(url, headers=headers, data='')
        response.raise_for_status()

        result = response.json()
        download_url = result.get("url")

        if not download_url:
            raise Exception("No download URL returned from API.")
        return download_url

    download_link = create_download_url(API_KEY, PROJECT_ID, file_id)
    print(f"\n Download URL:\n{download_link}")
