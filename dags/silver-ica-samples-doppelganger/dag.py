from datetime import datetime, timedelta
import pandas as pd
import io
import re
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

# Fetch environment variables
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER = Variable.get("S3_DWH_SILVER")
ICA_APIKEY = Variable.get("ICA_APIKEY")
prefix = "ica/samples/"

# Default DAG arguments
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
    'silver-ica-samples-doppelganger',
    default_args=default_args,
    description='ETL pipeline to merge and process CSV files from S3',
    schedule_interval=timedelta(days=1),
)

def get_latest_file(s3, bucket, prefix):
    """Fetch the latest CSV file from the given S3 bucket and prefix."""
    files = s3.list_keys(bucket_name=bucket, prefix=prefix)
    if not files:
        return None

    latest_file, latest_timestamp = None, None
    for file_key in files:
        if file_key.endswith('.csv'):
            file_obj = s3.get_key(key=file_key, bucket_name=bucket)
            if not latest_timestamp or file_obj.last_modified > latest_timestamp:
                latest_file, latest_timestamp = file_key, file_obj.last_modified
    return latest_file

def fetch_data(**kwargs):
    """Fetch and merge all CSV files from the Bronze S3 bucket."""
    s3 = S3Hook(aws_conn_id='aws')
    files = s3.list_keys(bucket_name=S3_DWH_BRONZE, prefix=prefix)
    if not files:
        raise ValueError(f"No files found in {prefix}")

    # Read and merge all CSV files from Bronze bucket
    merged_df = pd.concat(
        pd.read_csv(io.BytesIO(s3.get_key(key=file_key, bucket_name=S3_DWH_BRONZE).get()['Body'].read()))
        for file_key in files if file_key.endswith('.csv')
    )

    # Fetch latest file in Silver bucket
    latest_file = get_latest_file(s3, S3_DWH_SILVER, prefix)
    if latest_file:
        latest_csv = s3.get_key(key=latest_file, bucket_name=S3_DWH_SILVER).get()['Body'].read()
        df_silver = pd.read_csv(io.BytesIO(latest_csv))
        df_silver['date_create'] = pd.to_datetime(df_silver['date_create'], errors='coerce')
        latest_date_create = df_silver['date_create'].max()

        merged_df['timeCreated'] = pd.to_datetime(merged_df['timeCreated'], errors='coerce')
        merged_df = merged_df[merged_df['timeCreated'] >= latest_date_create]

    # Save merged data to S3 temporarily
    temp_key = f"{prefix}temp/filtered_data.csv"
    csv_buffer = io.StringIO()
    merged_df.to_csv(csv_buffer, index=False)
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=temp_key,
        bucket_name=S3_DWH_BRONZE,
        replace=True
    )
    return temp_key

def fetch_runname(id, headers):
    """Fetch runname from API."""
    url = f"https://ica.illumina.com/ica/rest/api/projects/87be74d8-dc18-4780-a96a-f976d380cc2e/samples/{id}/data?filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
    for _ in range(3):  # Retry up to 3 times
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                for item in data.get("items", []):
                    tags = item["details"].get("tags", {}).get("technicalTags", [])
                    project_name_tag = next((tag for tag in tags if "bssh.project.name" in tag), None)
                    if project_name_tag:
                        return project_name_tag.split(":")[1]  # Extract part after colon
                    else:
                        path = item["details"].get("path", "")
                        if "fastq.gz" in path:
                            match = re.search(r"(LP\d+-P\d+)", path)
                            if match:
                                return match.group(1)
            else:
                print(f"API responded with status code {response.status_code}")
        except requests.RequestException as e:
            print(f"Error fetching runname for {id}: {e}")
    return None

def transform_data(temp_key, **kwargs):
    """Transform and clean merged data."""
    s3 = S3Hook(aws_conn_id='aws')
    csv_obj = s3.get_key(key=temp_key, bucket_name=S3_DWH_BRONZE)
    filtered_data = csv_obj.get()['Body'].read().decode('utf-8')  # Decode bytes to string

    df = pd.read_csv(io.StringIO(filtered_data)).drop_duplicates()
    headers = {
        "accept": "application/vnd.illumina.v3+json",
        "X-API-Key": ICA_APIKEY
    }

    df['id_library'] = [fetch_runname(id, headers) for id in df['id']]
    df['id_repository'] = df['name']
    df['date_create'] = df['timeCreated']
    df['date_modify'] = df['timeModified']
    df = df[['id_repository', 'date_create', 'date_modify', 'id_library']]

    # Append transformed data to Silver bucket data
    latest_file = get_latest_file(s3, S3_DWH_SILVER, prefix)
    if latest_file:
        latest_csv = s3.get_key(key=latest_file, bucket_name=S3_DWH_SILVER).get()['Body'].read().decode('utf-8')  # Decode bytes
        df_silver = pd.read_csv(io.StringIO(latest_csv))
        main_df = pd.concat([df_silver, df], ignore_index=True)
    else:
        main_df = df

    # Save transformed data as CSV string
    csv_buffer = io.StringIO()
    main_df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def upload_to_s3(cleaned_data, **kwargs):
    """Upload the cleaned data to the Silver S3 bucket."""
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'{prefix}{data_interval_start.isoformat()}.csv'
    s3_key_latest = f'{prefix}latest.csv'

    s3 = S3Hook(aws_conn_id='aws')
    s3.load_string(string_data=cleaned_data, key=s3_key, bucket_name=S3_DWH_SILVER, replace=True)
    s3.load_string(string_data=cleaned_data, key=s3_key_latest, bucket_name=S3_DWH_SILVER, replace=True)

# Define tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'temp_key': '{{ task_instance.xcom_pull(task_ids="fetch_data") }}'},
    provide_context=True,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={'cleaned_data': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_data_task >> transform_data_task >> upload_to_s3_task
