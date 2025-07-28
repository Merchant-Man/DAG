import os
from datetime import datetime, timedelta
from typing import Dict, Any, Union, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.utils import fetch_and_dump, silver_transform_to_db
from utils.phenovar_transform import (
    transform_demography_data,
    transform_variable_data,
    transform_digital_consent_data,
    transform_data_sharing_data,
    transform_ethical_clearance_data,
    transform_form_data
)

AWS_CONN_ID = "aws"
PHENOVAR_CONN_ID = "phenovar-prod"
JWT_END_POINT = "api/v1/institution/login"
JWT_PAYLOAD = {
    "email": Variable.get("PHENOVAR_EMAIL"),
    "password": Variable.get("PHENOVAR_PASSWORD")
}
LOADER_PATH = "dags/repo/dags/include/loader"

# Endpoints and object paths
ENDPOINTS = {
    "demography": {
        "data_end_point": "api/v1/participants?perpage=50000",
        "object_path": "phenovar/participants",
        "query_file": "phenovar_particip_loader.sql"
    },
    "section": {
        "data_end_point": "api/v1/section?page=1&perpage=50000",
        "object_path": "phenovar/section",
        "query_file": "phenovar_section_loader.sql"
    },
    "category": {
        "data_end_point": "api/v1/category?page=1&perpage=50000",
        "object_path": "phenovar/category",
        "query_file": "phenovar_category_loader.sql"
    },
    "variable": {
        "data_end_point": "api/v1/variables?page=1&perpage=200000",
        "object_path": "phenovar/variable",
        "query_file": "phenovar_variable_loader.sql"
    },
    "form": {
        "data_end_point": "api/v1/form?page=1&perpage=200",
        "object_path": "phenovar/form",
        "query_file": "phenovar_form_loader.sql"
    },
    "data_sharing": {
        "data_end_point": "api/v1/participants/data-sharings?perpage=50000",
        "object_path": "phenovar/data-sharing",
        "query_file": "phenovar_data_sharing_loader.sql"
    },
    "digital_consent": {
        "data_end_point": "api/v1/digital-consent",
        "object_path": "phenovar/digital_consent",
        "query_file": "phenovar_digital_consent_loader.sql"
    },
    "ethical_clearance": {
        "data_end_point": "api/v1/ethical?perpage=100000",
        "object_path": "phenovar/ethical_clearance",  # inferred object path
        "query_file": "phenovar_ethical_clearance_loader.sql"
    }
}

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")

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
    max_active_runs=1,
    concurrency=3,
    catchup=False
)


def read_query(filename: str) -> str:
    with open(os.path.join(LOADER_PATH, filename)) as f:
        return f.read()


def get_token_function(resp: Dict[str, Any], response_header: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "headers": {
            "Authorization": f"Bearer {resp['data']['access_token']}",
            "Cookie": f"{response_header['Set-Cookie']}"
        }
    }


def phenovar_api_paginate(resp: Dict[str, Any]) -> Optional[int]:
    cur_page = resp["meta_data"]["page"]
    max_page = resp["meta_data"]["total_page"]
    print(f"Current page: {cur_page}, Max page: {max_page}")
    return cur_page + 1 if cur_page < max_page else None


# Preload the SQL queries
queries = {key: read_query(val["query_file"])
           for key, val in ENDPOINTS.items()}

# Common operator kwargs for fetch_and_dump
common_fetch_kwargs = {
    "jwt_end_point": JWT_END_POINT,
    "aws_conn_id": AWS_CONN_ID,
    "jwt_payload": JWT_PAYLOAD,
    "jwt_headers": {"Content-Type": "application/json"},
    "get_token_function": get_token_function,
    "response_key_data": "data",
    "curr_ds": "{{ ds }}",
    "prev_ds": "{{ prev_ds }}"
}

# Configurations for each task.
# For digital_consent, we need to include paging parameters.
fetch_and_dump_configs = {
    "demography": {
        "method_request": "GET",  # Use default GET since not specified
        "extra_kwargs": {}
    },
    "section": {
        "method_request": "GET",
        "extra_kwargs": {}
    },
    "category": {
        "method_request": "GET",
        "extra_kwargs": {}
    },
    "variable": {
        "method_request": "GET",
        "extra_kwargs": {}
    },
    "form": {
        "method_request": "GET",
        "extra_kwargs": {}
    },
    "data_sharing": {
        "method_request": "GET",
        "extra_kwargs": {}
    },
    "digital_consent": {
        "method_request": "POST",
        "extra_kwargs": {
            "data_payload": {"date": "{{ ds }}", "per_page": 50, "page": 1},
            "pagination_function": phenovar_api_paginate,
            "cursor_token_param": "page",
            "headers": {"Content-Type": "application/json"}
        }
    },
    "ethical_clearance": {
        "method_request": "GET",
        "extra_kwargs": {}
    }
}


def create_fetch_dump_task(key: str) -> PythonOperator:
    cfg = ENDPOINTS[key]
    task_id = f"bronze_fetch_jwt_and_dump_data_{key}"
    op_kwargs = {
        "api_conn_id": PHENOVAR_CONN_ID,
        "data_end_point": cfg["data_end_point"],
        "bucket_name": S3_DWH_BRONZE,
        "object_path": cfg["object_path"],
    }
    # Merge common kwargs and any extra kwargs (digital_consent has extra paging params)
    op_kwargs.update(common_fetch_kwargs)
    op_kwargs.update(fetch_and_dump_configs.get(
        key, {}).get("extra_kwargs", {}))
    if "method_request"  in fetch_and_dump_configs[key]:
        op_kwargs["method_request"] = fetch_and_dump_configs[key]["method_request"]

    return PythonOperator(
        task_id=task_id,
        python_callable=fetch_and_dump,
        dag=dag,
        op_kwargs=op_kwargs
    )


def create_silver_transform_task(key: str, transform_func: Any) -> PythonOperator:
    cfg = ENDPOINTS[key]
    task_id = f"silver_transform_{key}_to_db"
    return PythonOperator(
        task_id=task_id,
        python_callable=silver_transform_to_db,
        dag=dag,
        op_kwargs={
            "aws_conn_id": AWS_CONN_ID,
            "bucket_name": S3_DWH_BRONZE,
            "object_path": cfg["object_path"],
            "db_secret_url": RDS_SECRET,
            "transform_func": transform_func,
            "curr_ds": "{{ ds }}"
        },
        templates_dict={"insert_query": queries[key]},
        provide_context=True
    )


# Create fetch_and_dump tasks and the respective silver_transform tasks.
fetch_task_keys = ["demography", "category", "variable",
                   "digital_consent", "data_sharing", "ethical_clearance", "section", "form"]

tasks_fetch = {}
tasks_silver = {}

# Map transform functions (None if not needed)
transform_funcs = {
    "demography": transform_demography_data,
    "category": None,
    "variable": transform_variable_data,
    "digital_consent": transform_digital_consent_data,
    "data_sharing": transform_data_sharing_data,
    "ethical_clearance": transform_ethical_clearance_data,
    "section": None,
    "form": transform_form_data
}

for key in fetch_task_keys:
    tasks_fetch[key] = create_fetch_dump_task(key)
    tasks_silver[key] = create_silver_transform_task(key, transform_funcs[key])


# Define task dependencies
tasks_fetch["demography"] >> tasks_silver["demography"]  # type: ignore
tasks_fetch["category"] >> tasks_silver["category"]  # type: ignore
tasks_fetch["variable"] >> tasks_silver["variable"]  # type: ignore
tasks_fetch["digital_consent"] >> tasks_silver["digital_consent"] # type: ignore
tasks_fetch["data_sharing"] >> tasks_silver["data_sharing"]  # type: ignore
tasks_fetch["ethical_clearance"] >> tasks_silver["ethical_clearance"] # type: ignore
tasks_fetch["section"] >> tasks_silver["section"]  # type: ignore
tasks_fetch["form"] >> tasks_silver["form"]  # type: ignore 