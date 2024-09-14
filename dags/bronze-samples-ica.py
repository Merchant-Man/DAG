

from datetime import datetime, timedelta
import pandas as pd
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

ICA_APIKEY=Variable.get("ICA_APIKEY")
REGION=Variable.get("ICA_REGION")
ENDPOINT=f'/ica/rest/api/samples?region={REGION}&pageOffset=0&pageSize=600&sort=timeCreated%20asc'
S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bronze-samples-ica',
    default_args=default_args,
    description='An ETL pipeline to fetch data from an API, transform using Pandas, and load to S3',
    schedule_interval=timedelta(days=1),
)

def extract_transform_data(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    
    api_response = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_api')

    df = pd.DataFrame(json.loads(api_response)['items'])

    transformed_data = df.to_csv(f'/tmp/samples-ica-{data_interval_start.isoformat()}.csv', index=False)

def load_data(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start

    upload_to_s3_task = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename=f'/tmp/samples-ica-{data_interval_start.isoformat()}.csv',
        dest_key=f'samples/ica/samples-ica-{data_interval_start.isoformat()}.csv',
        dest_bucket=S3_DWH_BRONZE,
        aws_conn_id='aws',
        dag=dag,
    )

    return upload_to_s3_task.execute(context=kwargs)


fetch_data = SimpleHttpOperator(
    task_id='fetch_data_from_api',
    method='GET',
    http_conn_id='ica',
    endpoint=ENDPOINT,
    headers={
        "accept": "application/vnd.illumina.v3+json",
        "X-API-Key": ICA_APIKEY
        },
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=extract_transform_data,
    provide_context=True,
    dag=dag,
)

upload_to_s3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

fetch_data >> transform_data >> upload_to_s3

