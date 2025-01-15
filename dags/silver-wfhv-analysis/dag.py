from datetime import datetime, timedelta
import pandas as pd
import io
import ast

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER=Variable.get("S3_DWH_SILVER")
prefix="wfhv/analysis/"

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver-wfhv-analysis',
    default_args=default_args,
    description='ETL pipeline to merge CSV files from S3',
    schedule_interval=timedelta(days=1),
)

def fetch_data(**kwargs):
    s3 = S3Hook(aws_conn_id='aws')

    # List all objects in the S3 prefix
    files = s3.list_keys(bucket_name=S3_DWH_BRONZE, prefix=prefix)

    if not files:
        raise ValueError(f"No files found in {prefix}")

    # Filter for files with '.csv' extension
    csv_files = [file_key for file_key in files if file_key.endswith('.csv')]

    if not csv_files:
        raise ValueError(f"No CSV files found in {prefix}")

    # Sort files by timestamp extracted from their filenames
    csv_files.sort(key=lambda x: x.split('/')[-1].split('.csv')[0], reverse=True)

    # Get the latest file
    latest_file = csv_files[0]

    # Fetch the latest CSV file from S3
    csv_obj = s3.get_key(key=latest_file, bucket_name=S3_DWH_BRONZE)
    df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))

    # Convert DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    return csv_buffer.getvalue()


def transform_data(merged_data: str, **kwargs):
    # Read the CSV data into a DataFrame
    df = pd.read_csv(io.StringIO(merged_data))

    # Convert date_start to datetime for sorting
    df["date_start"] = pd.to_datetime(df["date_start"])

    # Keep only the latest record for each id_repository
    df = df.sort_values("date_start").drop_duplicates("id_repository", keep="last")

    # Convert cleaned DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    return csv_buffer.getvalue()

def upload_to_s3(cleaned_data, **kwargs):
    # Use data_interval_start for timestamp
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'{prefix}{data_interval_start.isoformat()}.csv'

    # Use S3Hook to upload the cleaned CSV to S3
    s3 = S3Hook(aws_conn_id='aws')

    # Upload the file with timestamp in the name
    s3.load_string(
        string_data=cleaned_data,
        key=s3_key,
        bucket_name=S3_DWH_SILVER,
        replace=True
    )

    # Upload the same file as 'latest.csv'
    s3_key_latest = f'{prefix}latest.csv'
    s3.load_string(
        string_data=cleaned_data,
        key=s3_key_latest,
        bucket_name=S3_DWH_SILVER,
        replace=True
    )

# Define tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # To pass kwargs
    op_kwargs={'merged_data': '{{ task_instance.xcom_pull(task_ids="fetch_data") }}'},
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,  # To pass kwargs
    op_kwargs={'cleaned_data': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    dag=dag,
)

# Set task dependencies
fetch_data_task >> transform_data_task >> upload_to_s3_task