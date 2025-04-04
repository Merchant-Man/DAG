from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import silver_transform_to_db
from airflow.operators.python import PythonOperator
from utils.phenovar_transform import fetch_documents_all_participants_and_dump, transform_document_data, flatten_document_data
import os
from typing import Dict, Any

AWS_CONN_ID = "aws"
PHENOVAR_CONN_ID = "phenovar-prod"
JWT_END_POINT = "api/v1/institution/login"
JWT_PAYLOAD = {
    "email": Variable.get("PHENOVAR_EMAIL"),
    "password": Variable.get("PHENOVAR_PASSWORD")
}


DOCUMENT_BY_ID_END_POINT = "api/v1/participants/document/{id}"

DOCUMENT_OBJECT_PATH = "phenovar/document"  # SHOULD CHANGE TODO
# OBJECT_PATH = "phenovar/demography" # SHOULD CHANGE TODO
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
DOCUMENT_LOADER_QEURY = "phenovar_document_loader.sql"


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
    "phenovar-documents",
    default_args=default_args,
    description="ETL pipeline for Phenovar Docuements API. Separated due to long processes",
    schedule_interval="0 0 * * 0",
    max_active_runs=1, # Only allowing one DAG run at a time
    concurrency=3, # Reduce the load on the phenovar server
    catchup=False
)


def get_token_function(resp: Dict[str, Any], response_header: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get token data from RegINA response.
    """
    return {
        "headers": {
            "Authorization": f"Bearer {resp['data']['access_token']}",
            "Cookie": f"{response_header['Set-Cookie']}"
        }
    }


with open(os.path.join("dags/repo/dags/include/loader", DOCUMENT_LOADER_QEURY)) as f:
    document_loader_query = f.read()


bronze_fetch_jwt_and_dump_data_document_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_dump_data_document",
    python_callable=fetch_documents_all_participants_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": PHENOVAR_CONN_ID,
        "data_end_point": DOCUMENT_BY_ID_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DOCUMENT_OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "db_secret_url": RDS_SECRET,
        "jwt_headers": {"Content-Type": "application/json"},
        "get_token_function": get_token_function,
        "response_key_data": "data",
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
    }
)

silver_transform_document_to_db_task = PythonOperator(
    task_id="silver_transform_document_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DOCUMENT_OBJECT_PATH,
        "db_secret_url": RDS_SECRET,
        "transform_func": transform_document_data,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": document_loader_query},
    provide_context=True
)

flatten_document_task = PythonOperator(
    task_id="flatten_document",
    python_callable=flatten_document_data,
    dag=dag,
    op_kwargs={
        "db_secret_url": RDS_SECRET,
        "verbose": True
    },
    provide_context=True
)

bronze_fetch_jwt_and_dump_data_document_task >> silver_transform_document_to_db_task >> flatten_document_task
