from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import silver_transform_to_db
from utils.regina_transform import fetch_documents_all_participants_and_dump, transform_document_data
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from typing import Dict, Any

AWS_CONN_ID="aws"
REGINA_CONN_ID="regina-prod"
JWT_END_POINT="api/v1/user/login"
DATA_END_POINT="api/v1/aql/query"
JWT_PAYLOAD={
    "username": Variable.get("REGINA_USERNAME"),
    "password": Variable.get("REGINA_PASSWORD")
    }
DATA_PAYLOAD={
    "query": "SELECT e/ehr_id/value AS ehrId, c0/uid/value AS compId, c0/name/value AS compName, c0 FROM EHR e[ehr_id/value='{id_repository}'] CONTAINS COMPOSITION c0"
}
OBJECT_PATH = "regina/documents" # SHOULD CHANGE TODO
S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "regina_document_loader.sql"


default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "regina-documents",
    default_args=default_args,
    description="ETL pipeline for RegINA clinical data using RegINA AQL API",
    schedule_interval="0 0 * * 6",
    catchup=False
)

def get_token_function(resp: Dict[str, Any], response_header: Dict[str, Any]) -> Dict[str, Any]: 
    """
    Get token data from RegINA response.
    """
    return {
        "headers": {
            "Authorization": f"Bearer {resp['data']['access_token']}",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*",
            "Content-Type": "application/json"
        }
    }

with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()

GET_ID_QUERY = "SELECT id_subject FROM regina_demography ORDER BY id_subject"

bronze_fetch_jwt_and_dump_data_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_data",
    python_callable=fetch_documents_all_participants_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": REGINA_CONN_ID,
        "data_end_point": DATA_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "db_secret_url": RDS_SECRET,
        "object_path": OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "data_payload": DATA_PAYLOAD,
        "get_id_query": GET_ID_QUERY,
        "jwt_headers": {"Content-Type": "application/json"},
        "response_key_data": ["data", "rows"],
        "get_token_function": get_token_function,
        "curr_ds": "{{ ds }}",
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
        "transform_func": transform_document_data, 
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": loader_query},
    provide_context=True
)

bronze_fetch_jwt_and_dump_data_task >> silver_transform_to_db_task