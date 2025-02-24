from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import fetch_and_dump, silver_transform_to_db
from airflow.operators.python import PythonOperator
import tenacity
import os
import pandas as pd

AWS_CONN_ID = "aws"
SIMBIOX_CONN_ID = "simbiox-prod"
TABLE_LIST = ["tbl_transfer", "tbl_transfer_formulir", "tbl_log_visit_biospc"]
API_TEMPLATE = "index.php/api/Table/get"
OBJECT_PATH_PREFIX = "simbiox"

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
SIMBIOX_APIKEY = Variable.get("SIMBIOX_APIKEY")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "simbiox_{table}_loader.sql"

def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts
    print(df.columns)
    df = df.astype(str)

    df.fillna(value="", inplace=True)
    return df


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
    "simbiox_tables",
    default_args=default_args,
    description="ETL pipeline for simbiox tables",
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2 #prevent the simbiox API to be exhausted
)

# tenacy max({min}, min(2^{try} * 1, {max})
# will do 32 secs 5 times and then 64, 128, 256, 512, 1024.
retry_args = dict(
    wait=tenacity.wait_exponential(multiplier=1, min=32, max=1024),
    stop=tenacity.stop_after_attempt(10),
)

for table in TABLE_LIST:
    DATA_END_POINT = f"{API_TEMPLATE}/{table}"
    OBJECT_PATH = f"{OBJECT_PATH_PREFIX}/{table}"

    with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY.format(table=table))) as f:
        loader_query = f.read()

    fetch_and_dump_task = PythonOperator(
        task_id=f"bronze_simbiox_{table}",
        python_callable=fetch_and_dump,
        dag=dag,
        op_kwargs={
            "api_conn_id": SIMBIOX_CONN_ID,
            "data_end_point": DATA_END_POINT,
            "aws_conn_id": AWS_CONN_ID,
            "bucket_name": S3_DWH_BRONZE,
            "object_path": OBJECT_PATH,
            "headers": {"api-key": SIMBIOX_APIKEY},
            "response_key_data": "data",
            "curr_ds": "{{ ds }}",
            "retry_args": retry_args,
            "limit": None  # bulk
        },
        provide_context=True
    )

    silver_transform_to_db_task = PythonOperator(
        task_id=f"silver_transform_{table}_to_db",
        python_callable=silver_transform_to_db,
        dag=dag,
        op_kwargs={
            "aws_conn_id": AWS_CONN_ID,
            "bucket_name": S3_DWH_BRONZE,
            "object_path": OBJECT_PATH,
            "db_secret_url": RDS_SECRET,
            "transform_func": transform_data,
            "curr_ds": "{{ ds }}"
        },
        templates_dict={"insert_query": loader_query},
        provide_context=True
    )

    fetch_and_dump_task >> silver_transform_to_db_task
