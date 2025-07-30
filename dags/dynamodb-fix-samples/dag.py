import os
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.operators.python import PythonOperator
from utils.utils import silver_transform_to_db
from utils.dynamodb_fix import fetch_dynamodb_and_load_to_s3, transform_fix_samples_data
from datetime import datetime, timedelta
from airflow.models import Variable

AWS_CONN_ID = "aws"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
DYNAMODB_TABLE = Variable.get("DYNAMODB_TABLE_FIX_SAMPLES")
LOADER_QUERY = "dynamodb_fix_samples_loader.sql"
FIX_OBJECT_PATH = "dynamodb/fix/samples"

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dynamodb_fix_samples', 
    default_args=default_args,
    description='ETL pipeline fetching data from DynamoDB and loading to S3',
    schedule_interval=timedelta(days=1),  
)

with open(os.path.join("dags/repo/dags/include/loader", LOADER_QUERY)) as f:
    loader_query = f.read()

# Define tasks
fetch_transform_and_load_task = PythonOperator(
    task_id='fetch_transform_and_load_to_s3',
    python_callable=fetch_dynamodb_and_load_to_s3,
    provide_context=True,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "dynamodb_table": DYNAMODB_TABLE,
        "bronze_bucket": S3_DWH_BRONZE,
        "bronze_object_path": FIX_OBJECT_PATH,
        "ds": "{{ ds }}"
    },
)

silver_fix_samples_transform_to_db_task = PythonOperator(
    task_id="silver_fix_samples_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": FIX_OBJECT_PATH,
        "transform_func": transform_fix_samples_data,
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": LOADER_QUERY},
    provide_context=True
)

fetch_transform_and_load_task >> silver_fix_samples_transform_to_db_task 
