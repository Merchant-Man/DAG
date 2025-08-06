import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.utils import silver_transform_to_db
from utils.dynamodb_fix import fetch_dynamodb_and_load_to_s3, transform_fix_samples_data, transform_fix_analysis_data
from datetime import datetime, timedelta
from airflow.models import Variable

AWS_CONN_ID = "aws"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
SAMPLES_DYNAMODB_TABLE = Variable.get("DYNAMODB_TABLE_FIX_SAMPLES")
SAMPLES_LOADER_QUERY = "dynamodb_fix_samples_loader.sql"
SAMPLES_FIX_OBJECT_PATH = "dynamodb/fix/samples"
ANALYSIS_DYNAMODB_TABLE = Variable.get("DYNAMODB_TABLE_FIX_ANALYSIS")
ANALYSIS_LOADER_QUERY = "dynamodb_fix_analysis_loader.sql"
ANALYSIS_FIX_OBJECT_PATH = "dynamodb/fix/analysis"

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dynamodb-fix-metadata', 
    default_args=default_args,
    description='ETL for fixing metadata from DynamoDB to S3 and RDS',
    schedule_interval=timedelta(days=1),  
)

with open(os.path.join("dags/repo/dags/include/loader", SAMPLES_LOADER_QUERY)) as f:
    samples_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", ANALYSIS_LOADER_QUERY)) as f:
    analysis_loader_query = f.read()

# Define tasks
samples_fix_fetch_transform_and_load_task = PythonOperator(
    task_id='samples_fix_fetch_transform_and_load',
    python_callable=fetch_dynamodb_and_load_to_s3,
    provide_context=True,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "dynamodb_table": SAMPLES_DYNAMODB_TABLE,
        "bronze_bucket": S3_DWH_BRONZE,
        "bronze_object_path": SAMPLES_FIX_OBJECT_PATH,
        "ds": "{{ ds }}"
    },
)

samples_fix_fetch_transform_to_db_task = PythonOperator(
    task_id="samples_fix_fetch_transform_to_db_task",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": SAMPLES_FIX_OBJECT_PATH,
        "transform_func": transform_fix_samples_data,
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": samples_loader_query},
    provide_context=True
)

analysis_fix_fetch_transform_and_load_task = PythonOperator(
    task_id='analysis_fix_fetch_transform_and_load_to_s3',
    python_callable=fetch_dynamodb_and_load_to_s3,
    provide_context=True,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "dynamodb_table": ANALYSIS_DYNAMODB_TABLE,
        "bronze_bucket": S3_DWH_BRONZE,
        "bronze_object_path": ANALYSIS_FIX_OBJECT_PATH,
        "ds": "{{ ds }}"
    },
)

analysis_fix_transform_to_db_task = PythonOperator(
    task_id="analysis_fix_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": ANALYSIS_FIX_OBJECT_PATH,
        "transform_func": transform_fix_analysis_data,
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": analysis_loader_query},
    provide_context=True
)

samples_fix_fetch_transform_and_load_task >> samples_fix_fetch_transform_to_db_task
analysis_fix_fetch_transform_and_load_task >> analysis_fix_transform_to_db_task
