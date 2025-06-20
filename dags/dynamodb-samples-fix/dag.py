from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.operators.python import PythonOperator
from utils.dynamodb_fix import fetch_dynamodb_and_load_to_s3
from datetime import datetime, timedelta
from airflow.models import Variable

AWS_CONN_ID = "aws"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
DYNAMODB_TABLE = Variable.get("DYNAMODB_TABLE")
FIX_OBJECT_PATH = "dynamodb/fix"

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dynamodb_fix', 
    default_args=default_args,
    description='ETL pipeline fetching data from DynamoDB and loading to S3',
    schedule_interval=timedelta(days=1),  
)

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

fetch_transform_and_load_task  
