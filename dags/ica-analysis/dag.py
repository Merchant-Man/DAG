from requests import Response
from datetime import datetime, timedelta
import pandas as pd
import os
from airflow import DAG
from airflow.models import Variable
from urllib.parse import urlparse
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from utils.utils import fetch_and_dump, silver_transform_to_db
import json
from typing import Dict, Any
import ast
import boto3
import re

AWS_CONN_ID = "aws"
ICA_CONN_ID = "ica"
ICA_APIKEY = Variable.get("ICA_APIKEY")
ICA_REGION = Variable.get("ICA_REGION")
ICA_OFFSET = Variable.get("ICA_OFFSET")
ICA_PROJECT = Variable.get("ICA_PROJECT")
DATA_END_POINT = f"/ica/rest/api/projects/{ICA_PROJECT}/analyses"
ICA_PAYLOAD = {
    "pageSize": 1000,  # Max page size of ICA API
    "pageToken": ""
    # The pageToken will be added to get the next page data
}
ICA_HEADERS = {
    "accept": "application/vnd.illumina.v3+json",
    "X-API-Key": ICA_APIKEY
}
OBJECT_PATH = "AF/ica/analysis"  # SHOULD CHANGE TODO
# OBJECT_PATH = "ica/analysis" # SHOULD CHANGE TODO
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "ica_analysis_loader.sql"

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
    'ica-analysis',
    default_args=default_args,
    description='ETL pipeline for fetching ICA analyses data using ICA API',
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


def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates

    # Remove duplicates from the main DataFrame
    df = df.drop_duplicates()

    # TODO It might good to filter the delta based on timeModified and/or timeCreated

    # Clean up
    # Split the user_reference into id_repository basede on conditoin
    def split_user(ref:str):
        if re.match(r"^SKI", ref):
            # For SKI_XXX_YYY get the SKI_XXXX
            return "_".join(ref.split("_")[0:2])
        elif re.match(r".*(_M|_T)$", ref):
            # For TOP UP 1H0044101C02_250205_M
            return ref
        elif re.match(r"DRAGEN", ref):
            return ref.split("-")[0]
        return ref.split("_")[0]
    
    df["id_repository"] = df["userReference"].apply(split_user)
    # New regex for extracting id_batch
    df["id_batch"] = df["tags"].apply(lambda x: re.search(r"(LP[\w\d]+)", str(ast.literal_eval(x)["userTags"])))
    df["pipeline_name"] = df["pipeline"].apply(
        lambda x: ast.literal_eval(x)["code"])
    df["pipeline_type"] = "secondary"

    def _split_reference(ref: str) -> str:
        """
        Handling edgecases for the path of CRAM and VCF from Illumina Analysis "reference" field.
        """
        temp_code_repo = ""
        if re.match(r"SKI_.*", ref):
            # SKI_3175140_9efb681c-DRAGEN_Germline_WGS_4-2-7_sw-mode-JK-8b97ceb8-ab78-489a-8d48-901f1b240c79
            temp_code_repo = re.search(r"SKI_[^_]+", ref)[0]
        elif re.match(r"[^_]+_[\d]{6}_M_.*", ref):
            # 0C0123801C03_250209_M_1a75a295-ede8212c-9b83-4029-bade-7c19493b2fe7
            temp_code_repo = re.search(r"[^_]+_[\d]{6}_[TM]{1}", ref)[0]
        elif re.match(r"[^_-]+-1-DRAGEN-4-2-6-Germline.*", ref):
            # 0H0066801C01-1-DRAGEN-4-2-6-Germline-All-Callers-DRAGEN_Germline_WGS_4-2-6-v2_sw-mode-JK-b0a743a0-4762-436d-a988-4cd2be474910
            temp_code_repo = re.search(r"[^_-]+", ref)[0]
        else:
            # 0C0067101C03_8b688247-DRAGEN_Germline_WGS_4-2-7_sw-mode-JK-df9ac94b-dbe8-4eb9-b377-6b0946661c4a
            temp_code_repo = ref.split('_')[0]
        
        return temp_code_repo

    df["cram"] = df["reference"].apply(
        lambda x: (lambda ref: f"s3://bgsi-data-illumina/pro/analysis/{x}/{ref}/{ref}.cram")(_split_reference(x)))
    df["vcf"] = df["reference"].apply(
        lambda x: (lambda ref: f"s3://bgsi-data-illumina/pro/analysis/{x}/{ref}/{ref}.hard-filtered.vcf.gz")(_split_reference(x)))
    df["tags"] = df["tags"].apply(ast.literal_eval)
    df = df.join(pd.json_normalize(df["tags"]).add_prefix("tags_"))

    rename_map = {
        # old: new
        "timeCreated": "time_created",
        "timeModified": "time_modified",
        "startDate": "date_start",
        "endDate": "date_end",
        "userReference": "run_name",
        "status": "run_status",
        "tags_technicalTags": "tag_technical_tags",
        "tags_userTags": "tag_user_tags",
        "tags_referenceTags": "tag_reference_tags"
    }
    df.rename(columns=rename_map, inplace=True)

    s3_size = boto3.client("s3",
                           aws_access_key_id=Connection.get_connection_from_secrets(
                               AWS_CONN_ID).login,
                           aws_secret_access_key=Connection.get_connection_from_secrets(AWS_CONN_ID).password)
    # Add file sizes
    # NOTE: IF YOUR AWS ACCOUNT DON'T HAVE ACCESS IT WILL GIVE 0.

    def get_file_size(s3_path):
        try:
            # Parse the S3 URL
            parsed_url = urlparse(s3_path)
            bucket_name = parsed_url.netloc
            # Extract key (path inside the bucket)
            key = parsed_url.path.lstrip("/")

            # Get the size of the object
            response = s3_size.head_object(Bucket=bucket_name, Key=key)
            return response["ContentLength"]  # Size in bytes
        except Exception as e:
            # print(f"Error fetching size for {s3_path}: {e}")
            return None

    df["cram_size"] = df["cram"].apply(get_file_size)
    df["vcf_size"] = df["vcf"].apply(get_file_size)

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df.fillna(value="", inplace=True)

    df = df[["id", "time_created", "time_modified", "created_at", "updated_at", "id_repository", "id_batch", "date_start",
             "date_end", "pipeline_name", "pipeline_type", "run_name", "run_status", "cram", "cram_size", "vcf", "vcf_size", "tag_technical_tags", "tag_user_tags", "tag_reference_tags"]]
    
    # Even we remove duplicates, API might contain duplicate records for an id_subject
    # So, we will keep the latest record by id (unique)
    df['time_modified'] = pd.to_datetime(df['time_modified'])
    df = df.sort_values('time_modified').groupby('id').tail(1)
    df = df.astype(str)
    return df


fetch_and_dump_task = PythonOperator(
    task_id="bronze_ica_analysis",
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
