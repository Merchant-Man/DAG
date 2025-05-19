from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import fetch_and_dump, silver_transform_to_db
from airflow.operators.python import PythonOperator
import pandas as pd
import tenacity
import os

AWS_CONN_ID = "aws"
SIMBIOX_CONN_ID = "simbiox-prod"
DATA_END_POINT = "index.php/api/Table/get/tbl_data_biosample"
OBJECT_PATH = "AF/simbiox/tbl_data_biosample"  # SHOULD CHANGE TODO
# OBJECT_PATH = "simbiox/tbl_data_biosample" # SHOULD CHANGE TODO
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
SIMBIOX_APIKEY = Variable.get("SIMBIOX_APIKEY")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "simbiox_biosamples_loader.sql"


default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    cols = {"patient_id": "id_patient",
            "repository_code": "code_repository",
            "box_code": "code_box", "position_code": "code_position",
            "received_date": "date_received", "enter_date_in_biorepo": "date_enumerated",
            "sample_old_code": "origin_code_repository",
            "sample_box_old_code": "origin_code_box", "sample_type_id": "biosample_type",
            "specimen_type_id": "biosample_specimen", "vol_product": "biosample_volume",
            "status": "biosample_status"}

    df.rename(columns=cols, inplace=True)

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    df.rename(columns={"dateofbirth": "date_of_birth"}, inplace=True)

    df = df[[
        "id", "id_patient", "code_repository", "code_box", "code_position", "date_received", "date_enumerated", "id_biobank", "origin_code_repository",
        "origin_code_box", "biosample_type", "biosample_specimen", "type_case", "sub_cell_specimen",  "biosample_volume", "biosample_status", "created_at", "updated_at", 
        "final_result_analytic", "tube_type", "degrees_celsius_storage", "research_id", "project_id", "last_save", "remove", "log_biospc_id", "kode_kedatangan", "kode_hub_penyimpan"]]

    df = df.astype(str)

    # Need to fillna so that the mysql connector can insert the data.
    df.fillna(value="", inplace=True)
    print(df.columns)
    return df


dag = DAG(
    "simbiox-biosamples",
    default_args=default_args,
    description="ETL pipeline for Simbiox Biosamples API data (tbl_data_biosample)",
    schedule_interval="0 1 * * *",  # @1 AM everyday
    catchup=False,
)

with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()

# tenacy max({min}, min(2^{try} * 1, {max})
# will do 32 secs 5 times and then 64, 128, 256, 512, 1024.
retry_args = dict(
    wait=tenacity.wait_exponential(multiplier=1, min=32, max=1024),
    stop=tenacity.stop_after_attempt(10),
)

fetch_and_dump_task = PythonOperator(
    task_id="bronze_simbiox_biosamples",
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
