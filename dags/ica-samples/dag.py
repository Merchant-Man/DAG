from requests import Response
import requests
from datetime import datetime, timedelta
import pandas as pd
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils.utils import fetch_and_dump, silver_transform_to_db
import json
from typing import Dict, Any
import ast
import re
import time

AWS_CONN_ID = "aws"
ICA_CONN_ID = "ica"
ICA_APIKEY = Variable.get("ICA_APIKEY")
ICA_REGION = Variable.get("ICA_REGION")
ICA_OFFSET = Variable.get("ICA_OFFSET")
ICA_PROJECT = Variable.get("ICA_PROJECT")
DATA_END_POINT = f"/ica/rest/api/samples"
ICA_PAYLOAD = {
    "region": ICA_REGION,
    "pageSize": 1000,  # Max page size of ICA API
    "pageToken": ""
    # The pageToken will be added to get the next page data
}
ICA_HEADERS = {
    "accept": "application/vnd.illumina.v3+json",
    "X-API-Key": ICA_APIKEY
}
OBJECT_PATH = "AF/ica/samples"  # SHOULD CHANGE TODO
# OBJECT_PATH = "ica/samples" # SHOULD CHANGE TODO
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "ica_samples_loader.sql"

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'ica-samples',
    default_args=default_args,
    description='ETL pipeline for fetching ICA samples data using ICA API',
    schedule_interval=timedelta(days=1),
    catchup=False
)

with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()


def ica_paginate(response: Dict[str, Any]) -> str:
    """
    Paginating function for ICA API. It consumes request.Response object where it checks the next token request. If the token can't be found or there is no more token, it will return empty string.
    """
    pageToken = ""
    try:
        pageToken = response["nextPageToken"]
    except json.JSONDecodeError as e:
        print(e)
        raise ValueError(
            "The response can't be parsed to JSON file. Please check the response payload!")

    return pageToken


def fetch_runname(id, headers):
    """Fetch runname from API."""
    url = f"https://ica.illumina.com/ica/rest/api/projects/{ICA_PROJECT}/samples/{id}/data?filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
    for i in range(3):  # Retry up to 3 times
        try:
            # add timeout to handle connection issue
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                for item in data.get("items", []):
                    tags = item["details"].get(
                        "tags", {}).get("technicalTags", [])
                    project_name_tag = next(
                        (tag for tag in tags if "bssh.project.name" in tag), None)
                    if project_name_tag:
                        # Extract part after colon
                        return project_name_tag.split(":")[1]
                    path = item["details"].get("path", "")
                    if "fastq.gz" in path:
                        match = re.search(r"(LP\d+-P\d+)", path)
                        if match:
                            return match.group(1)
            else:
                print(
                    f"API responded with status code {response.status_code} for id {id} with details: {response.json().get('detail')}")
                break
        except requests.RequestException as e:
            time_to_sleep = 32 * (2 ** i)
            print(
                f"Error fetching runname for {id}: {e}. Trying exponential backoff for {time_to_sleep} seconds")
            time.sleep(time_to_sleep)  # Exponential backoff
    return None


def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    print("=== Starting to fetch the id library ===")
    df["id_library"] = [fetch_runname(id, ICA_HEADERS) for id in df["id"]]
    print("=== Fetching id library is finished ===")

    df["application"] = df["application"].apply(ast.literal_eval)
    df = df.join(pd.json_normalize(
        df["application"]).add_prefix("application_"))
    df["tags"] = df["tags"].apply(ast.literal_eval)
    df = df.join(pd.json_normalize(df["tags"]).add_prefix("tags_"))

    rename_map = {
        # old: new
        "name": "id_repository",
        "timeCreated": "time_created",
        "timeModified": "time_modified",
        "ownerId": "owner_id",
        "tenantId": "tenant_id",
        "tenantName": "tenant_name",
        "tags_technicalTags": "tag_technical_tags",
        "tags_userTags": "tag_user_tags",
        "tags_connectorTags": "tag_connetor_tags",
        "tags_runInTags": "tag_run_in_tags"
    }

    df.rename(columns=rename_map, inplace=True)

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df.fillna(value="", inplace=True)
    df = df[["id", "id_library", "time_created", "time_modified", "created_at", "updated_at", "owner_id", "tenant_id", "tenant_name",
             "id_repository", "status", "tag_technical_tags", "tag_user_tags", "tag_connetor_tags", "tag_run_in_tags", "application_id", "application_name"]]

    # Even we remove duplicates, API might contain duplicate records for an id_subject
    # So, we will keep the latest record by id (unique)
    df['time_modified'] = pd.to_datetime(df['time_modified'])
    df = df.sort_values('time_modified').groupby('id').tail(1)

    df = df.astype(str)
    return df


fetch_and_dump_task = PythonOperator(
    task_id="bronze_ica_samples",
    python_callable=fetch_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": ICA_CONN_ID,
        "data_end_point": DATA_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": OBJECT_PATH,
        "headers": ICA_HEADERS,
        "data_payload": ICA_PAYLOAD,
        "response_key_data": "items",
        "pagination_function": ica_paginate,
        "cursor_token_param": "pageToken",
        "curr_ds": "{{ ds }}"  # curr_ds for file naming
        # "limit_param":"pageSize", # limit as default (1000)
        # "limit": 1000
    },
    provide_context=True
)

silver_transform_to_db_task = PythonOperator(
    task_id="silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": OBJECT_PATH,
        "transform_func": transform_data,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": loader_query},
    provide_context=True
)

fetch_and_dump_task >> silver_transform_to_db_task
