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
from typing import Any, Dict, Tuple, Optional, List
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


def fetch_runname(id: str, headers: dict) -> Optional[Tuple[Optional[Any], List[str], List[str], List[str], List[str], List[str], List[str]]]:
    """Fetch runname from API."""
    url = f"https://ica.illumina.com/ica/rest/api/projects/{ICA_PROJECT}/samples/{id}/data?filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
    for i in range(3):  # Retry up to 3 times
        try:
            # add timeout to handle connection issue
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                for item in data.get("items", []):
                    technical_tags = item["details"].get(
                        "tags", {}).get("technicalTags", [])
                    user_tags = item["details"].get(
                        "tags", {}).get("userTags", [])
                    connector_tags = item["details"].get(
                        "tags", {}).get("connectorTags", [])
                    run_in_tags = item["details"].get(
                        "tags", {}).get("runInTags", [])
                    run_out_tags = item["details"].get(
                        "tags", {}).get("runOutTags", [])
                    reference_tags = item["details"].get(
                        "tags", {}).get("referenceTags", [])
                    project_name_tag = next(
                        (tag for tag in technical_tags if "bssh.run.name" in tag), None)
                    return project_name_tag, technical_tags, user_tags, connector_tags, run_in_tags, run_out_tags, reference_tags
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

    # Convert timeModified to datetime and filter out rows before today
    print(f"Rows before filter: {len(df)}")
    df["timeModified"] = pd.to_datetime(df["timeModified"])
    df["timeCreated"] = pd.to_datetime(df["timeCreated"])
    # From ICA API the timeModified is in UTC and the Airflow run in UTC
    ts = pd.to_datetime(ts).tz_localize("UTC")
    td = pd.Timedelta(1, "days")
    ts_1 = ts - td
    print(f"ts: {ts}, td: {td}, ts_1: {ts_1}")
    df = df.loc[(((df["timeModified"] <= ts) & (df["timeModified"] >= ts_1)) | (
        (df["timeCreated"] <= ts) & (df["timeCreated"] >= ts_1)))]
    print(f"Rows after filter: {len(df)}")

    # Check if filtered_df is not empty before processing
    if df.empty:
        return pd.DataFrame()

    print("=== Starting to fetch the id library ===")

    def safe_fetch(sample_id):
        result = fetch_runname(sample_id, ICA_HEADERS)
        if result is None:
            # Return a tuple of Nones for the 7 expected values
            return (None, None, None, None, None, None, None)
        return result

    temp_api = [safe_fetch(sample_id) for sample_id in df["id"]]

    temp_api = pd.DataFrame(
        temp_api,
        columns=[
            "id_library",
            "sample_list_technical_tags",
            "sample_list_user_tags",
            "sample_list_connector_tags",
            "sample_list_run_in_tags",
            "sample_list_run_out_tags",
            "sample_list_reference_tags",
        ],
        index=df.index
    )
    print("=== Fetching id library is finished ===")
    df = pd.concat([df, temp_api], axis=1)

    df["application"] = df["application"].apply(ast.literal_eval)
    df = df.join(pd.json_normalize(
        df["application"].tolist()).add_prefix("application_"))
    df["tags"] = df["tags"].apply(ast.literal_eval)
    df = df.join(pd.json_normalize(df["tags"].tolist()).add_prefix("tags_"))

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
             "id_repository", "status", "tag_technical_tags", "tag_user_tags", "tag_connetor_tags", "tag_run_in_tags", "application_id", "application_name",
            "sample_list_technical_tags", "sample_list_user_tags", "sample_list_connector_tags", "sample_list_run_in_tags", "sample_list_run_out_tags", "sample_list_reference_tags"]]

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

fetch_and_dump_task >> silver_transform_to_db_task # type: ignore
