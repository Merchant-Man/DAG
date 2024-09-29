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
prefix="ica/analysis/"

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
    'template_s3_multi',
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
    
    all_data_frames = []

    for file_key in files:
        if file_key.endswith('.csv'):
            # Read each CSV file into a DataFrame
            csv_obj = s3.get_key(key=file_key, bucket_name=S3_DWH_BRONZE)
            df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
            all_data_frames.append(df)

    # Merge all DataFrames into one
    merged_df = pd.concat(all_data_frames, ignore_index=True)

    # Fetch df2, df3, df4 CSVs from S3
    df2 = pd.read_csv(io.BytesIO(s3.get_key(key=df2_key, bucket_name=S3_DWH_BRONZE).get()['Body'].read()))
    df3 = pd.read_csv(io.BytesIO(s3.get_key(key=df3_key, bucket_name=S3_DWH_BRONZE).get()['Body'].read()))
    df4 = pd.read_csv(io.BytesIO(s3.get_key(key=df4_key, bucket_name=S3_DWH_BRONZE).get()['Body'].read()))
    
    # Convert merged DataFrame to CSV format
    csv_buffer = io.StringIO()
    merged_df.to_csv(csv_buffer, index=False)

    # Return the data and additional DataFrames for merging
    return {
        'merged_data': csv_buffer.getvalue(),
        'df2': df2.to_csv(index=False),
        'df3': df3.to_csv(index=False),
        'df4': df4.to_csv(index=False),
    }

def transform_data(merged_data: dict, **kwargs):
    # Extract the merged data and additional DataFrames from kwargs
    merged_csv = merged_data['merged_data']
    
    # Read all CSV data into DataFrames
    df = pd.read_csv(io.StringIO(merged_csv))
    df2 = pd.read_csv(io.StringIO(merged_data['df2']))
    # df3 = pd.read_csv(io.StringIO(merged_data['df3']))
    # df4 = pd.read_csv(io.StringIO(merged_data['df4']))

    # Remove duplicates from the main DataFrame
    df = df.drop_duplicates()

    # Merge with df2, df3, and df4
    df = df.merge(df2, on='common_column', how='inner')
    # df = df.merge(df3, on='common_column', how='inner')
    # df = df.merge(df4, on='common_column', how='inner')

    # Convert cleaned DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    return csv_buffer.getvalue()

def upload_to_s3(**kwargs):
    # Get merged data from previous task
    merged_data = kwargs['ti'].xcom_pull(task_ids='fetch_data')

    # Clean and remap the DataFrame
    cleaned_data = transform_data(merged_data)

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
    dag=dag,
)

# Set task dependencies
fetch_data_task >> transform_data_task >> upload_to_s3_task

