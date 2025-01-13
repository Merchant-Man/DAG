from datetime import datetime, timedelta
import pandas as pd
import io
import ast

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER = Variable.get("S3_DWH_SILVER")
prefix = "zlims/samples/"

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver-zlims-samples',
    default_args=default_args,
    description='ETL pipeline to process latest ZLIMS samples file from S3',
    schedule_interval=timedelta(days=5),
)

def fetch_data(**kwargs):
    s3 = S3Hook(aws_conn_id='aws')
    
    # List all objects in the S3 prefix
    files = s3.list_keys(bucket_name=S3_DWH_BRONZE, prefix=prefix)
    
    if not files:
        raise ValueError(f"No files found in {prefix}")
    
    # Filter for CSV files and get the latest one by timestamp
    csv_files = [f for f in files if f.endswith('.csv')]
    if not csv_files:
        raise ValueError(f"No CSV files found in {prefix}")
        
    latest_file = sorted(csv_files)[-1]
    print(f"Processing latest file: {latest_file}")
    
    # Read only the latest CSV file
    csv_obj = s3.get_key(key=latest_file, bucket_name=S3_DWH_BRONZE)
    df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
    print(f"Loaded {len(df)} rows from {latest_file}")
    
    # Convert DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    return csv_buffer.getvalue()

def transform_data(merged_data: str, **kwargs):
    # Read the CSV data into a DataFrame
    df = pd.read_csv(io.StringIO(merged_data))
    print(f"Initial rows in transform: {len(df)}")
    
    # Remove duplicates
    df = df.drop_duplicates()
    print(f"Rows after deduplication: {len(df)}")

    # Rename columns
    cols = {
        'Sample ID(*)': 'id_repository',
        'Flowcell ID': 'id_library',
        'Library ID': 'id_pool',
        'DNB ID(*)': 'id_dnb',
        'Flowcell ID': 'id_flowcell',
        'Index(*)': 'id_index',
        'Create Time': 'date_create',
    }

    df.rename(columns=cols, inplace=True)
    
    # Convert index to numeric, handling errors
    df['id_index'] = pd.to_numeric(df['id_index'], errors='coerce').astype('Int64')
    
    # Select only the columns we want
    df = df[list(cols.values())]

    # Filter out test/sample data
    patterns = r'(?i)Test|test|tes|^BC|SAMPLE|^DNB'
    df = df[~df['id_repository'].str.contains(patterns, na=False)]

    # Clean id_repository field
    df['id_repository'] = df['id_repository'].str.split(r'[-_]').str[0]
    
    print(f"Final rows after transformation: {len(df)}")

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
    
    print(f"Successfully uploaded to {s3_key} and {s3_key_latest}")

# Define tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    op_kwargs={'merged_data': '{{ task_instance.xcom_pull(task_ids="fetch_data") }}'},
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    op_kwargs={'cleaned_data': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    dag=dag,
)

# Set task dependencies
fetch_data_task >> transform_data_task >> upload_to_s3_task