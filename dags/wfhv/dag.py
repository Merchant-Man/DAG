from datetime import datetime, timedelta
import os
from utils.wfhv_transform import transform_analysis_data, transform_qc_data, transform_samples_data
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils.utils import silver_transform_to_db, dynamo_and_dump
from utils.wfhv import fetch_wfhv_samples_dump_data, fetch_wfhv_analysis_dump_data, fetch_wfhv_stats_dump_data

AWS_CONN_ID = "aws"
QC_OBJECT_PATH = "wfhv/qc"
SAMPLES_OBJECT_PATH = "wfhv/samples"
ANALYSIS_OBJECT_PATH = "wfhv/analysis"
WFHV_INPUT_BUCKET = "bgsi-data-wfhv-input"
WFHV_OUTPUT_BUCKET = "bgsi-data-wfhv-output"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
QC_LOADER_QEURY = "wfhv_qc_loader.sql"
SAMPLES_LOADER_QEURY = "wfhv_samples_loader.sql"
ANALYSIS_LOADER_QEURY = "wfhv_analysis_loader.sql"

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    'wfhv-pl',
    default_args=default_args,
    description='ETL pipeline for fetching WFHV PL QC, Samples and Analysis from Nextflow pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False
)

with open(os.path.join("dags/repo/dags/include/loader", QC_LOADER_QEURY)) as f:
    qc_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", SAMPLES_LOADER_QEURY)) as f:
    samples_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", ANALYSIS_LOADER_QEURY)) as f:
    analysis_loader_query = f.read()


qc_bronze_s3_to_s3_task = PythonOperator(
    task_id="qc_bronze_s3_to_s3",
    python_callable=fetch_wfhv_stats_dump_data,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "wfhv_output_bucket": WFHV_OUTPUT_BUCKET,
        "bronze_bucket": S3_DWH_BRONZE,
        "bronze_object_path": QC_OBJECT_PATH,
        "curr_ds": "{{ ds }}"
    },
    provide_context=True
)

qc_silver_transform_to_db_task = PythonOperator(
    task_id="qc_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": QC_OBJECT_PATH,
        "transform_func": transform_qc_data,
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": qc_loader_query},
    provide_context=True
)

samples_bronze_s3_to_s3_task = PythonOperator(
    task_id="samples_bronze_s3_to_s3",
    python_callable=fetch_wfhv_samples_dump_data,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bronze_bucket": S3_DWH_BRONZE,
        "bronze_object_path": SAMPLES_OBJECT_PATH,
        "wfhv_input_bucket": WFHV_INPUT_BUCKET,
        "curr_ds": "{{ ds }}"
    },
    provide_context=True
)

samples_silver_transform_to_db_task = PythonOperator(
    task_id="samples_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": SAMPLES_OBJECT_PATH,
        "transform_func": transform_samples_data,
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": samples_loader_query},
    provide_context=True
)

analysis_bronze_s3_to_s3_task = PythonOperator(
    task_id="analysis_bronze_s3_to_s3",
    python_callable=fetch_wfhv_analysis_dump_data,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "wfhv_output_bucket": WFHV_OUTPUT_BUCKET,
        "bronze_bucket": S3_DWH_BRONZE,
        "bronze_object_path": ANALYSIS_OBJECT_PATH,
        "curr_ds": "{{ ds }}"
    },
    provide_context=True
)

analysis_silver_transform_to_db_task = PythonOperator(
    task_id="analysis_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": ANALYSIS_OBJECT_PATH,
        "transform_func": transform_analysis_data,
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": analysis_loader_query},
    provide_context=True
)

samples_bronze_s3_to_s3_task >> samples_silver_transform_to_db_task
analysis_bronze_s3_to_s3_task >> analysis_silver_transform_to_db_task
qc_bronze_s3_to_s3_task >> qc_silver_transform_to_db_task
