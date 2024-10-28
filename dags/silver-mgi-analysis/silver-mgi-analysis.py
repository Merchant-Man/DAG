from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER = Variable.get("S3_DWH_SILVER")
prefix = "mgi/analysis/"

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver-mgi-analysis',
    default_args=default_args,
    description='Copy most recent analysis file to latest.csv',
    schedule_interval=timedelta(days=1),
)

def fetch_and_upload_latest(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws')
    bronze_files = s3_hook.list_keys(
        bucket_name=S3_DWH_BRONZE,
        prefix=prefix,
        delimiter='/'
    )
    
    csv_files = [f for f in bronze_files if f.endswith('.csv')]
    if not csv_files:
        raise AirflowException(f"No CSV files found in {S3_DWH_BRONZE}/{prefix}")
    
    latest_file = sorted(csv_files)[-1]
    latest_content = s3_hook.read_key(
        key=latest_file,
        bucket_name=S3_DWH_BRONZE
    )
    
    s3_hook.load_string(
        string_data=latest_content,
        key=f"{prefix}latest.csv",
        bucket_name=S3_DWH_SILVER,
        replace=True
    )

with dag:
    PythonOperator(
        task_id='fetch_and_upload_latest',
        python_callable=fetch_and_upload_latest,
    )