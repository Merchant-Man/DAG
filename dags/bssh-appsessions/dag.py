from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta
import io
import re
import os
from utils.utils import silver_transform_to_db
from utils.bssh import fetch_bclconvert_and_dump, fetch_bclconvertDemux_and_dump
# from utils.bssh_tra import transform_appsessions_data

# ----------------------------
# Constants and Config
# ----------------------------

AWS_CONN_ID = "aws"
BSSH_CONN_ID = "bssh"

API_BASE = "https://api.aps4.sh.basespace.illumina.com/v2"
BSSH_APIKEY = Variable.get("BSSH_APIKEY1")
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
# Updated OBJECT_PATH to match what silver_transform_to_db expects
BCLCONVERT_OBJECT_PATH = "bssh/appsessions"
LOADER_QUERY_PATH = "illumina_appsession_loader.sql"

API_KEY = Variable.get("API_KEY_ICA")
PROJECT_ID = Variable.get("ICA_Project_id")
BASE_URL = "https://ica.illumina.com/ica/rest/api"
BSSH_APIKEY = Variable.get("BSSH_APIKEY1")
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
# Updated OBJECT_PATH to match what silver_transform_to_db expects
DEMUX_OBJECT_PATH = "bssh/demux"


# ----------------------------
# DAG Definition
# ----------------------------

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'bssh',
    default_args=default_args,
    description='Fetch QC from BSSH',
    schedule_interval=timedelta(days=1),
    catchup=False
)
with open(os.path.join("dags/repo/dags/include/loader", LOADER_QUERY_PATH)) as f:
    loader_query = f.read()

# Bronze task
bclconvert_qs_fetch_and_dump_task = PythonOperator(
    task_id="bronze_fetch_bssh_bclconvert_appsessions",
    python_callable=fetch_bclconvert_and_dump,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": BCLCONVERT_OBJECT_PATH,
        "api_base": API_BASE
    },
    provide_context=True
)

# bclconvert_transform_to_db_task = PythonOperator(
#     task_id="analysis_silver_transform_to_db",
#     python_callable=silver_transform_to_db,
#     dag=dag,
#     op_kwargs={
#         "aws_conn_id": AWS_CONN_ID,
#         "bucket_name": S3_DWH_BRONZE,
#         "object_path": BCLCONVERT_OBJECT_PATH,
#         "db_secret_url": RDS_SECRET,
#         "transform_func": transform_appsessions_data,
#         "multi_files": True,
#         "curr_ds": "{{ ds }}"},
#     templates_dict={"insert_query": loader_query},
#     provide_context=True
# )

demux_qs_fetch_and_dump_task = PythonOperator(
    task_id="bronze_fetch_bssh_bclconvertandQC",
    python_callable=fetch_bclconvertDemux_and_dump,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": "bssh/final_output",
        # "transform_func": transform_data,
        "API_KEY": API_KEY,
        "BASE_URL": BASE_URL, 
        "PROJECT_ID": PROJECT_ID
    },
    provide_context=True
)

# demux_upsert_appsessions = PythonOperator(
#     task_id="silver_upsert_bclconvert_appsessions",
#     python_callable=silver_transform_to_db,
#     dag=dag,
#     op_kwargs={
#         "aws_conn_id": AWS_CONN_ID,
#         "bucket_name": BUCKET_NAME,
#         "object_path": "bssh/final_output",        # read from your dumped final output
#         "transform_func": transform_appsession_data,  # <-- use the appsession transform here
#         "db_secret_url": RDS_SECRET,
#         "curr_ds": "{{ ds }}",
#     },
#     templates_dict={"insert_query": loader_query},   # <-- illumina_appsession_loader.sql
#     provide_context=True,
# )
# DAG flow
bclconvert_qs_fetch_and_dump_task 
demux_qs_fetch_and_dump_task
# fetch_and_dump_task 
