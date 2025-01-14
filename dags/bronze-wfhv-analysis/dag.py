from datetime import datetime, timedelta, timezone
import pandas as pd
from io import StringIO
import boto3
import re
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")

bucket_name="bgsi-data-wfhv-output"
include_patterns = [
    r".*cram$",
    r".*_snp\.vcf\.gz$",
    r".*wf-human-alignment-report\.html$"
]

def match_patterns(file_name, patterns):
    matches = [None] * len(patterns)
    for i, pattern in enumerate(patterns):
        if re.match(pattern, file_name):
            matches[i] = file_name
    return matches

def extract_pipeline_version_and_type(html_content):
    match = re.search(r'wf-human-alignment-report.html</code> nextflow workflow \(([\d.]+)\)', html_content)
    version = match.group(1) if match else None
    pipeline_type = "secondary"
    return version, pipeline_type

def get_html_file_content(bucket, key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        raise AirflowException(f"Error reading HTML file {key}: {e}")

def process_s3_data(bucket_name):
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        folders = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', []) if 'CommonPrefixes' in response]
    except Exception as e:
        raise AirflowException(f"Error listing objects in S3 bucket {bucket_name}: {e}")

    data = []
    for folder in folders:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        files = response.get('Contents', [])

        row = {
            'id_repository': folder.split('.')[0].split('_')[0],
            'run_name': folder.rstrip('/'),
            'cram': None,
            'cram_size': None,
            'vcf': None,
            'vcf_size': None,
            'pipeline_name': None,
            'pipeline_type': None,
            'date_start': None
        }

        date_start = None
        for file in files:
            file_name = file['Key']
            matches = match_patterns(file_name, include_patterns)

            if matches[0]:
                row['cram'] = f"s3://{bucket_name}/{matches[0]}"
                row['cram_size'] = str(int(file['Size']))
            if matches[1]:
                row['vcf'] = f"s3://{bucket_name}/{matches[1]}"
                row['vcf_size'] = str(int(file['Size']))
            if matches[2]:
                html_content = get_html_file_content(bucket_name, file_name)
                version, pipeline_type = extract_pipeline_version_and_type(html_content)
                row['pipeline_name'] = f"wf-human-variation {version}"
                row['pipeline_type'] = pipeline_type

            if not date_start or file['LastModified'] < date_start:
                date_start = file['LastModified']

        row['date_start'] = date_start.astimezone(timezone.utc).isoformat()
        data.append(row)

    df = pd.DataFrame(data)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()

def load_data(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='process_s3_data')
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'wfhv/analysis/{data_interval_start.isoformat()}.csv'

    s3 = S3Hook(aws_conn_id='aws')
    s3.load_string(
        string_data=transformed_data,
        key=s3_key,
        bucket_name=S3_DWH_BRONZE,
        replace=True
    )

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze-wfhv-analysis',
    default_args=default_args,
    description='ETL ONT analysis output',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 13),
    catchup=False,
)

process_task = PythonOperator(
    task_id='process_s3_data',
    python_callable=process_s3_data,
    op_kwargs={'bucket_name': bucket_name},
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

process_task >> upload_to_s3_task
