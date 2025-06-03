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
BSSH_CONN_ID = "bssh"
BSSH_APIKEY = Variable.get("BSSH_APIKEY")
DATA_END_POINT = f"/biosamples"
BSSH_PAYLOAD = {
    
}
BSSH_HEADERS = {
    "Authorization": f"Bearer {BSSH_APIKEY}",
    "Accept": "application/json"
}

OBJECT_PATH = "bssh/biosample"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "ica_analysis_loader.sql"

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
    'bssh-biosample',
    default_args=default_args,
    description='ETL pipeline for fetching BSSH biosample data using BSSH API',
    schedule_interval=timedelta(days=1),
    catchup=False
)

with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()

def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:

    # Remove duplicates from the main DataFrame
    df = df.drop_duplicates()

    df = df.astype(str)
    return df


fetch_and_dump_task = PythonOperator(
    task_id="bronze_ica_analysis",
    python_callable=fetch_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": BSSH_CONN_ID,
        "data_end_point": DATA_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": OBJECT_PATH,
        "headers": BSSH_HEADERS,
        "data_payload": {},  # or your actual payload
        "transform_func": transform_data,
        "curr_ds": "{{ ds }}",  # curr_ds for file naming
        "limit": 1000
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
