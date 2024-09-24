from datetime import datetime, timedelta
import pandas as pd
import io

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

# Configure the S3 bucket and prefix
S3_BUCKET = 'bgsi-data-dev'  # Set your S3 bucket name in Airflow Variables
S3_PREFIX = 'airflow/'  # Set the S3 prefix to read from
MERGED_CSV_KEY = 'airflow/merged/merged_data.csv'  # Set the S3 key for the merged file

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'template_s3',
    default_args=default_args,
    description='ETL pipeline to merge CSV files from S3',
    schedule_interval=timedelta(days=1),
)

def read_and_merge_csv(**kwargs):
    s3 = S3Hook(aws_conn_id='aws')
    # List all objects in the S3 prefix
    files = s3.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)

    all_data_frames = []

    for file_key in files:
        if file_key.endswith('.csv'):
            # Read each CSV file into a DataFrame
            csv_obj = s3.get_key(key=file_key, bucket_name=S3_BUCKET)
            df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
            all_data_frames.append(df)

    # Merge all DataFrames into one
    merged_df = pd.concat(all_data_frames, ignore_index=True)

    # Convert merged DataFrame to CSV format
    csv_buffer = io.StringIO()
    merged_df.to_csv(csv_buffer, index=False)

    return csv_buffer.getvalue()

def upload_merged_csv_to_s3(**kwargs):
    merged_data = kwargs['ti'].xcom_pull(task_ids='read_and_merge_csv')

    # Use S3Hook to upload the merged CSV to S3
    s3 = S3Hook(aws_conn_id='aws')
    s3.load_string(
        string_data=merged_data,
        key=MERGED_CSV_KEY,
        bucket_name=S3_BUCKET,
        replace=True
    )

# Define tasks
read_and_merge = PythonOperator(
    task_id='read_and_merge_csv',
    python_callable=read_and_merge_csv,
    provide_context=True,
    dag=dag,
)

upload_to_s3 = PythonOperator(
    task_id='upload_merged_csv',
    python_callable=upload_merged_csv_to_s3,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
read_and_merge >> upload_to_s3
