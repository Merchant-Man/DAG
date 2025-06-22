import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import silver_transform_to_db
from airflow.operators.python import PythonOperator
from utils.pgx import get_pgx_report_and_dump, transform_pgx_logs_data, get_pgx_detail_to_dwh

AWS_CONN_ID = "aws"
OBJECT_PATH = "pgx/report_logs"
INPUT_BUCKET_NAME = "nl-data-pgx-input"
OUTPUT_BUCKET_NAME = "nl-data-pgx-output"
OUTPUT_PATH_TEMPLATE = "production/{sample_id}/report_{report_type}/production.{sample_id}.{report_type}.data.json"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
SIMBIOX_APIKEY = Variable.get("SIMBIOX_APIKEY")
RDS_SECRET = Variable.get("RDS_SECRET")
REPORT_STATUS_LOADER_QEURY = "staging_pgx_report_status.sql"
REPORT_DETAIL_LOADER_QEURY = "dwh_restricted_pgx_detail_report.sql"

default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 22),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "pgx_report",
    default_args=default_args,
    description="ETL pipeline for getting the report of PGx data",
    schedule_interval=timedelta(days=1),
    catchup=False
)

with open(os.path.join("dags/repo/dags/include/loader", REPORT_STATUS_LOADER_QEURY)) as f:
    staging_pgx_report_status_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", REPORT_DETAIL_LOADER_QEURY)) as f:
    pgx_report_detail_loader_query = f.read()

fetch_and_dump_pgx_report_metadata_task = PythonOperator(
    task_id="bronze_pgx",
    python_callable=get_pgx_report_and_dump,
    dag=dag,
    op_kwargs={
        "input_bucket_name": INPUT_BUCKET_NAME,
        "output_bucket_name": OUTPUT_BUCKET_NAME,
        "dwh_bucket_name": S3_DWH_BRONZE,
        "object_path": OBJECT_PATH,
        "aws_conn_id": AWS_CONN_ID,
        "db_secret_url": RDS_SECRET,
        "output_path_template": OUTPUT_PATH_TEMPLATE,
        "curr_ds": "{{ ds }}",
    },
    provide_context=True
)

silver_transform_to_db_pgx_report_metadata_task = PythonOperator(
    task_id="silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": OBJECT_PATH,
        "transform_func": transform_pgx_logs_data,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": staging_pgx_report_status_loader_query},
    provide_context=True
)

fetch_and_dump_pgx_report_detail_task = PythonOperator(
    task_id="bronze_pgx_detail",
    python_callable=get_pgx_detail_to_dwh,
    dag=dag,
    op_kwargs={
        "pgx_report_output_bucket": OUTPUT_BUCKET_NAME,
        "aws_conn_id": AWS_CONN_ID,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}",
    },
    templates_dict={"insert_query": pgx_report_detail_loader_query},
    provide_context=True
)

fetch_and_dump_pgx_report_metadata_task >> silver_transform_to_db_pgx_report_metadata_task >> fetch_and_dump_pgx_report_detail_task  # type: ignore
