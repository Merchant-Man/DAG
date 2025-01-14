from datetime import datetime, timedelta, timezone
import pandas as pd
import json
from io import StringIO, BytesIO

import boto3
import re
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Airflow variable for the S3 bucket name
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
bucket_name = 'bgsi-data-wfhv-output'

# Initialize the S3 client
s3 = boto3.client('s3')

def extract_stats_data(bucket_name, **kwargs):
    """
    Processes `.stats.json` files in an S3 bucket, extracting and flattening their data.

    Args:
        bucket_name (str): Name of the S3 bucket to process.

    Returns:
        pd.DataFrame: A DataFrame containing the processed data from the `.stats.json` files.
    """
    
    # List all folders in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
    folders = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]
    
    all_data = []

    # Iterate over each folder
    for folder in folders:
        # List all files in the current folder
        folder_contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        stats_files = [
            obj['Key'] for obj in folder_contents.get('Contents', [])
            if obj['Key'].endswith('.stats.json')
        ]
        
        for stats_file in stats_files:
            try:
                print(f"Processing file: {stats_file}")
                
                # Fetch and parse the .stats.json file
                obj = s3.get_object(Bucket=bucket_name, Key=stats_file)
                file_content = obj['Body'].read().decode('utf-8')
                data = json.loads(file_content)
                
                # Flatten specific fields
                if "Yield (reads >=Nbp)" in data:
                    yield_reads = data.pop("Yield (reads >=Nbp)")
                    for key, value in yield_reads.items():
                        data[f"Yield (reads >={key}bp)"] = value
                
                if "Bases with >=N-fold coverage" in data:
                    bases_coverage = data.pop("Bases with >=N-fold coverage")
                    for key, value in bases_coverage.items():
                        data[f"Bases with >={key}-fold coverage"] = value
                
                # Add metadata for traceability
                data['run_name'] = folder.rstrip('/')
                data['file_name'] = stats_file.split('/')[-1]
                
                all_data.append(data)
            except Exception as e:
                print(f"Could not process {stats_file}: {e}")

    # Convert the data into a DataFrame
    df = pd.DataFrame(all_data)
    
    # Convert all columns to strings
    df = df.astype(str)
    
    # Extract additional metadata
    df['id_repository'] = df['run_name'].apply(lambda x: x.split('_')[0])
    
    # Convert DataFrame to CSV format
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Return the CSV data as string
    return csv_buffer.getvalue()

def load_data(**kwargs):
    # Retrieve the transformed CSV from XCom
    transformed_data = kwargs['ti'].xcom_pull(task_ids='extract_stats_data')
    
    # Generate S3 key using data_interval_start
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'wfhv/qc/{data_interval_start.isoformat()}.csv'
    
    # Use S3Hook to upload the data to S3
    s3 = S3Hook(aws_conn_id='aws')
    s3.load_string(
        string_data=transformed_data,
        key=s3_key,
        bucket_name=S3_DWH_BRONZE,
        replace=True
    )
    
# DAG Default Args
default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition must occur after all required functions and variables are declared
dag = DAG(
    'bronze-wfhv-qc',
    default_args=default_args,
    description='A DAG to extract ont qc data from stats.json',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 13),
    catchup=False,
)

# PythonOperator for processing S3 objects
extract_stats_data_task = PythonOperator(
    task_id='extract_stats_data',
    python_callable=extract_stats_data,
    op_args=[bucket_name],  # Pass bucket_name here
    dag=dag,
)
# PythonOperator for uploading to S3
upload_to_s3_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,  # Ensure this DAG object is defined
)

# Define dependencies
extract_stats_data_task >> upload_to_s3_task
