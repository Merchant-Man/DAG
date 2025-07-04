from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import fetch_and_dump, silver_transform_to_db
from airflow.operators.python import PythonOperator
from utils.phenovar_transform import transform_demography_data, transform_variable_data, transform_digital_consent_data, transform_data_sharing_data
import os
from typing import Dict, Any, Union

AWS_CONN_ID = "aws"
PHENOVAR_CONN_ID = "phenovar-prod"
JWT_END_POINT = "api/v1/institution/login"
JWT_PAYLOAD = {
    "email": Variable.get("PHENOVAR_EMAIL"),
    "password": Variable.get("PHENOVAR_PASSWORD")
}

# current limit of our programs is 20k
DEMOGRAPHY_END_POINT = "api/v1/participants?perpage=50000"
CATEGORY_END_POINT = "api/v1/category?page=1&perpage=50000"
VARIABLE_END_POINT = "api/v1/variables?page=1&perpage=200000"
DATA_SHARING_END_POINT = "api/v1/participants/data-sharings?perpage=50000"
DIGITAL_CONSENT_END_POINT = "api/v1/digital-consent"  # Per page defined on the body

DEMOGRAPHY_OBJECT_PATH = "phenovar/participants"
CATEGORY_OBJECT_PATH = "phenovar/category"
VARIABLE_OBJECT_PATH = "phenovar/variable"
DATA_SHARING_OBJECT_PATH = "phenovar/data-sharing"
DIGITAL_CONSENT_OBJECT_PATH = "phenovar/digital_consent"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
DEMOGRAPHY_LOADER_QEURY = "phenovar_particip_loader.sql"
CATEGORY_LOADER_QEURY = "phenovar_category_loader.sql"
VARIABLE_LOADER_QEURY = "phenovar_variable_loader.sql"
DATA_SHARING_LOADER_QEURY = "phenovar_data_sharing_loader.sql"
DIGITAL_CONSENT_LOADER_QEURY = "phenovar_digital_consent_loader.sql"


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
    "phenovar",
    default_args=default_args,
    description="ETL pipeline for Phenovar participants data using Phenovar API",
    schedule_interval=timedelta(days=1),
    max_active_runs=1,  # Only allowing one DAG run at a time
    concurrency=3,  # Reduce the load on the phenovar server
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


def phenovar_api_paginate(resp: Dict[str, Any]) -> Union[str, None]:
    """
    Get page data from Phenovar response.
    """
    cur_page = resp["meta_data"]["page"]
    max_page = resp["meta_data"]["total_page"]
    print(f"Current page: {cur_page}, Max page: {max_page}")
    if cur_page < max_page:
        return cur_page + 1
    return None


with open(os.path.join("dags/repo/dags/include/loader", DEMOGRAPHY_LOADER_QEURY)) as f:
    demography_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", CATEGORY_LOADER_QEURY)) as f:
    category_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", VARIABLE_LOADER_QEURY)) as f:
    variable_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", DIGITAL_CONSENT_LOADER_QEURY)) as f:
    digital_consent_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", DATA_SHARING_LOADER_QEURY)) as f:
    data_sharing_loader_query = f.read()

bronze_fetch_jwt_and_dump_data_digital_consent_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_dump_data_digital_consent",
    python_callable=fetch_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": PHENOVAR_CONN_ID,
        "data_end_point": DIGITAL_CONSENT_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DIGITAL_CONSENT_OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "data_payload": {
            "date": "{{ ds }}",
            "per_page": 50,
            "page": 1
        },
        "pagination_function": phenovar_api_paginate,
        "cursor_token_param": "page",
        "method_request": "POST",
        "jwt_headers": {"Content-Type": "application/json"},
        "headers": {"Content-Type": "application/json"},
        "get_token_function": get_token_function,
        "response_key_data": "data",
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
    }
)

silver_transform_digital_consent_to_db_task = PythonOperator(
    task_id="silver_transform_digital_consent_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DIGITAL_CONSENT_OBJECT_PATH,
        "transform_func": transform_digital_consent_data,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": digital_consent_loader_query},
    provide_context=True
)

bronze_fetch_jwt_and_dump_data_demography_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_dump_data_demography",
    python_callable=fetch_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": PHENOVAR_CONN_ID,
        "data_end_point": DEMOGRAPHY_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DEMOGRAPHY_OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "jwt_headers": {"Content-Type": "application/json"},
        "get_token_function": get_token_function,
        "response_key_data": "data",
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
    }
)

silver_transform_demography_to_db_task = PythonOperator(
    task_id="silver_transform_demography_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DEMOGRAPHY_OBJECT_PATH,
        "transform_func": transform_demography_data,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": demography_loader_query},
    provide_context=True
)

bronze_fetch_jwt_and_dump_data_category_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_dump_data_category",
    python_callable=fetch_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": PHENOVAR_CONN_ID,
        "data_end_point": CATEGORY_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": CATEGORY_OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "jwt_headers": {"Content-Type": "application/json"},
        "get_token_function": get_token_function,
        "response_key_data": "data",
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
    }
)

silver_transform_category_to_db_task = PythonOperator(
    task_id="silver_transform_category_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": CATEGORY_OBJECT_PATH,
        "db_secret_url": RDS_SECRET,
        "transform_func": None,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": category_loader_query},
    provide_context=True
)


bronze_fetch_jwt_and_dump_data_variable_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_dump_data_variable",
    python_callable=fetch_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": PHENOVAR_CONN_ID,
        "data_end_point": VARIABLE_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": VARIABLE_OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "jwt_headers": {"Content-Type": "application/json"},
        "get_token_function": get_token_function,
        "response_key_data": "data",
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
    }
)

silver_transform_variable_to_db_task = PythonOperator(
    task_id="silver_transform_variable_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": VARIABLE_OBJECT_PATH,
        "db_secret_url": RDS_SECRET,
        "transform_func": transform_variable_data,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": variable_loader_query},
    provide_context=True
)


bronze_fetch_jwt_and_dump_data_data_sharing_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_dump_data_data_sharing",
    python_callable=fetch_and_dump,
    dag=dag,
    op_kwargs={
        "api_conn_id": PHENOVAR_CONN_ID,
        "data_end_point": DATA_SHARING_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DATA_SHARING_OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "jwt_headers": {"Content-Type": "application/json"},
        "get_token_function": get_token_function,
        "response_key_data": "data",
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
    }
)

silver_transform_data_sharing_to_db_task = PythonOperator(
    task_id="silver_transform_data_sharing_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DATA_SHARING_OBJECT_PATH,
        "db_secret_url": RDS_SECRET,
        "transform_func": transform_data_sharing_data,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": data_sharing_loader_query},
    provide_context=True
)

bronze_fetch_jwt_and_dump_data_demography_task >> silver_transform_demography_to_db_task  # type: ignore
bronze_fetch_jwt_and_dump_data_category_task >> silver_transform_category_to_db_task  # type: ignore
bronze_fetch_jwt_and_dump_data_variable_task >> silver_transform_variable_to_db_task  # type: ignore
bronze_fetch_jwt_and_dump_data_digital_consent_task >> silver_transform_digital_consent_to_db_task  # type: ignore
bronze_fetch_jwt_and_dump_data_data_sharing_task >> silver_transform_data_sharing_to_db_task  # type: ignore
