from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import fetch_and_dump, silver_transform_to_db
from utils.regina_transform import transform_demography_data
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from typing import Dict, Any

AWS_CONN_ID="aws"
REGINA_CONN_ID="regina-prod"
JWT_END_POINT="api/v1/user/login"
DATA_END_POINT="api/v2/registry/demography/list?limit=20000"
JWT_PAYLOAD={
    "username": Variable.get("REGINA_USERNAME"),
    "password": Variable.get("REGINA_PASSWORD")
    }
OBJECT_PATH = "AF/regina/demography" # SHOULD CHANGE TODO
# OBJECT_PATH = "regina/demography" # SHOULD CHANGE TODO
S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "regina_demog_loader.sql"


default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "regina-demography",
    default_args=default_args,
    description="ETL pipeline for RegINA demography data using RegINA demography API",
    schedule_interval=timedelta(days=1),
    catchup=False
)

def get_token_function(resp: Dict[str, Any], response_header: Dict[str, Any]) -> Dict[str, Any]: 
    """
    Get token data from RegINA response.
    """
    return {
        "headers": {
            "Authorization": f"Bearer {resp['data']['access_token']}"
        }
    }


with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()

bronze_fetch_jwt_and_dump_data_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_data",
    python_callable=fetch_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": REGINA_CONN_ID,
        "data_end_point": DATA_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE, 
        "object_path": OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "jwt_headers": {"Content-Type": "application/json"},
        "response_key_data": ["data", "records"],
        "get_token_function": get_token_function,
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
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
        "transform_func": transform_demography_data, 
        "multi_files": True,
        "db_secret_url": RDS_SECRET, 
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": loader_query},
    provide_context=True
)

bronze_fetch_jwt_and_dump_data_task >> silver_transform_to_db_task