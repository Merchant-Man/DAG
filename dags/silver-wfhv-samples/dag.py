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
prefix = "wfhv/samples/"

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
    'silver-wfhv-samples',
    default_args=default_args,
    description='ETL pipeline to process latest ONT samples file from S3',
    schedule_interval=timedelta(days=5),
)

def fetch_data(**kwargs):
    s3 = S3Hook(aws_conn_id='aws')
    
    # List all objects in the S3 prefix
    files = s3.list_keys(bucket_name=S3_DWH_BRONZE, prefix=prefix)
    
    # Filter for CSV files and get the latest one by timestamp
    csv_files = [f for f in files if f.endswith('.csv')]

    latest_file = sorted(csv_files)[-1]

    # Read only the latest CSV file
    csv_obj = s3.get_key(key=latest_file, bucket_name=S3_DWH_BRONZE)
    df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
 
    # Convert DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    return csv_buffer.getvalue()

def transform_data(merged_data: str, **kwargs):
    # Read the CSV data into a DataFrame
    df = pd.read_csv(io.StringIO(merged_data))

    # Remove duplicates
    df = df.drop_duplicates()

    # Ensure the column contains datetime objects
    df['date_upload'] = pd.to_datetime(df['date_upload'])

    # Convert to UTC and format as ISO 8601
    df['date_upload'] = df['date_upload'].dt.tz_localize('UTC').apply(lambda x: x.isoformat())

    # Assuming df is your DataFrame
    grouped_df = df.groupby('id_repository').agg({
        'alias': list,  # Aggregate 'alias' as a list
        'total_passed_bases': list,  # Aggregate 'total_passed_bases' as a list
        'bam_size': list,  # Aggregate 'bam_size' as a list
        'date_upload': list,  # Aggregate 'date_upload' as a list
        'total_bases': list,  # Aggregate 'total_bases' as a list
        'passed_bases_percent': list,  # Aggregate 'passed_bases_percent' as a list
        'bam_folder': list,  # Aggregate 'bam_folder' as a list
        'id_library': list  # Aggregate 'id_library' as a list
    }).reset_index()

    # Add a column for the sum of total_passed_bases
    grouped_df['sum_of_total_passed_bases'] = grouped_df['total_passed_bases'].apply(
        lambda x: sum(filter(None, [float(i) for i in x if pd.notnull(i)]))
    ).astype(str)

    # Add a column for the sum of bam_size
    grouped_df['sum_of_bam_size'] = grouped_df['bam_size'].apply(
        lambda x: sum(filter(None, [int(i) for i in x if pd.notnull(i)]))
    ).astype(str)

    # Convert cleaned DataFrame to CSV format
    csv_buffer = io.StringIO()
    grouped_df.to_csv(csv_buffer, index=False)

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