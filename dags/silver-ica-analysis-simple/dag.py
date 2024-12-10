from datetime import datetime, timedelta
import pandas as pd
import io
import ast
import boto3
from urllib.parse import urlparse

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
    'start_date': datetime(2024, 11, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver-ica-analysis-simple',
    default_args=default_args,
    description='ETL pipeline to merge CSV files from S3',
    schedule_interval=timedelta(days=1),
)

def etl_to_s3(**kwargs):
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
    df = pd.concat(all_data_frames, ignore_index=True)

    # Remove duplicates from the main DataFrame
    df = df.drop_duplicates()

    # Clean up
    df['id_repository'] = df['userReference'].str.split('_').str[0]
    df['id_batch']      = df['tags'].apply(lambda x: ast.literal_eval(x)['userTags'][4] if len(ast.literal_eval(x)['userTags']) > 4 else None)
    df['date_start']    = df['startDate']
    df['date_end']      = df['endDate']
    df['pipeline_name'] = df['pipeline'].apply(lambda x: ast.literal_eval(x)['code'])
    df['pipeline_type'] = 'secondary'
    df['run_name']      = df['userReference']
    df['run_status']    = df['status']
    df['cram']          = df['reference'].apply(lambda x: f"s3://bgsi-data-illumina/pro/analysis/{x}/{x.split('_')[0]}/{x.split('_')[0]}.cram")
    df['vcf']           = df["reference"].apply(lambda x: f"s3://bgsi-data-illumina/pro/analysis/{x}/{x.split('_')[0]}/{x.split('_')[0]}.hard-filtered.vcf.gz")

    s3_size = boto3.client('s3')
    # Add file sizes
    def get_file_size(s3_path):
        try:
            # Parse the S3 URL
            parsed_url = urlparse(s3_path)
            bucket_name = parsed_url.netloc
            key = parsed_url.path.lstrip('/')  # Extract key (path inside the bucket)

            # Get the size of the object
            response = s3_size.head_object(Bucket=bucket_name, Key=key)
            return response['ContentLength']  # Size in bytes
        except Exception as e:
            # print(f"Error fetching size for {s3_path}: {e}")
            return None
        
    df['cram_size']        = df['cram'].apply(get_file_size)
    df['vcf_size']         = df['vcf'].apply(get_file_size)
    df = df[['id_repository', 'id_batch', 'date_start', 'date_end', 'pipeline_name', 'pipeline_type', 'run_name', 'run_status','cram', 'cram_size', 'vcf', 'vcf_size']].copy()
    
    # Convert cleaned DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    cleaned_data = csv_buffer.getvalue()

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


etl_to_s3_task = PythonOperator(
    task_id='etl_to_s3',
    python_callable=etl_to_s3,
    provide_context=True,  # To pass kwargs
    dag=dag,
)

etl_to_s3_task

