from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta
from utils.utils import silver_transform_to_db
from utils.bssh import fetch_bclconvert_runs_with_flowcell_yield_and_dump, fetch_demux_qs_ica_to_s3
from utils.illumina_transform import transform_bssh_data, transform_demux_data, transform_qs_data
import os
# ----------------------------
# Constants and Config
# ----------------------------

AWS_CONN_ID = "aws"
RDS_SECRET = Variable.get("RDS_SECRET")
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")

API_BASE = "https://api.aps4.sh.basespace.illumina.com/v2"
BASE_URL = "https://ica.illumina.com/ica/rest/api"
BSSH_APIKEY = Variable.get("API_KEY_BSSH")
API_KEY = Variable.get("API_KEY_ICA")
PROJECT_ID = Variable.get("ICA_PROJECT_BSSH_ID")

BSSH_RUN_OBJECT_PATH = "illumina/bssh/runs/"
BSSH_BIOSAMPLE_OBJECT_PATH = "illumina/bssh/biosamples/"
DEMUX_OBJECT_PATH = "illumina/demux"
QS_OBJECT_PATH = "illumina/qs"

BSSH_RUN_LOADER_QUERY = "illumina_bssh_loader.sql"
BSSH_BIOSAMPLE_LOADER_QUERY = "illumina_bssh_loader.sql"
DEMUX_LOADER_QUERY = "illumina_demux_loader.sql"
QS_LOADER_QUERY = "illumina_qs_loader.sql"

# ----------------------------
# DAG Definition
# ----------------------------

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
    'bssh',
    default_args=default_args,
    description='Fetch QC from BSSH',
    schedule_interval=timedelta(days=1),
    catchup=False
)

with open(os.path.join("dags/repo/dags/include/loader", BSSH_RUN_LOADER_QUERY)) as f:
    bssh_run_loader_query = f.read()
with open(os.path.join("dags/repo/dags/include/loader", BSSH_BIOSAMPLE_LOADER_QUERY)) as f:
    bssh_biosample_loader_query = f.read()
with open(os.path.join("dags/repo/dags/include/loader", DEMUX_LOADER_QUERY)) as f:
    demux_loader_query = f.read()
with open(os.path.join("dags/repo/dags/include/loader", QS_LOADER_QUERY)) as f:
    qs_loader_query = f.read()

# Bronze task
bssh_fetch_and_dump_task = PythonOperator(
    task_id="bronze_fetch_bssh_and_dump",
    python_callable=fetch_bclconvert_runs_with_flowcell_yield_and_dump,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "api_base": API_BASE,
        "api_token": BSSH_APIKEY,
        "bucket_name": S3_DWH_BRONZE,
        "run_object_path": BSSH_RUN_OBJECT_PATH,
        "biosample_object_path": BSSH_BIOSAMPLE_OBJECT_PATH
    },
    provide_context=True
)

demux_qs_fetch_and_dump_task = PythonOperator(
    task_id="bronze_fetch_bssh_bclconvertandQC",
    python_callable=fetch_demux_qs_ica_to_s3,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "API_KEY": API_KEY,
        "BASE_URL": API_BASE, 
        "PROJECT_ID": PROJECT_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path_demux": DEMUX_OBJECT_PATH,
        "object_path_qs": QS_OBJECT_PATH ,

    },
    provide_context=True
)

bssh_run_silver_transform_to_db_task = PythonOperator(
    task_id="bssh_run_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": BSSH_RUN_OBJECT_PATH,
        "db_secret_url": RDS_SECRET,
        "all_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": bssh_run_loader_query},
    provide_context=True
)

bssh_biosample_silver_transform_to_db_task = PythonOperator(
    task_id="bssh_biosample_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": BSSH_RUN_OBJECT_PATH,
        "db_secret_url": RDS_SECRET,
        "all_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": bssh_biosample_loader_query},
    provide_context=True
)

demux_silver_transform_to_db_task = PythonOperator(
    task_id="demux_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": DEMUX_OBJECT_PATH,
        "transform_func": transform_demux_data,
        "db_secret_url": RDS_SECRET,
        "all_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": demux_loader_query},
    provide_context=True
)

qs_silver_transform_to_db_task = PythonOperator(
    task_id="qs_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": QS_OBJECT_PATH,
        "transform_func": transform_qs_data,
        "db_secret_url": RDS_SECRET,
        "all_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": qs_loader_query},
    provide_context=True
)