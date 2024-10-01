from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
SIMBIOX_APIKEY=Variable.get("SIMBIOX_APIKEY")

default_args = {
    'owner': 'bgsi_data',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bronze-simbiox-tbl_data_biosample-simple',
    default_args=default_args,
    description='ETL pipeline using an API',
    schedule_interval=timedelta(days=5),
)

def etl_to_s3(**kwargs):
    # EXTRACT
    http_conn_id = 'https://simbiox.kemkes.go.id'
    api_key = SIMBIOX_APIKEY
    endpoint = 'index.php/api/Table/get/tbl_data_biosample'
    response = requests.get(f'{http_conn_id}/{endpoint}', headers={"api-key": api_key})
    api_response = response.json()
    
    # TRANSFORM
    df = pd.DataFrame(api_response)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # LOAD
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'simbiox/tbl_data_biosample/{data_interval_start.isoformat()}.csv'
    
    s3 = S3Hook(aws_conn_id='aws')
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=s3_key,
        bucket_name=S3_DWH_BRONZE,
        replace=True
    )

etl_to_s3_task = PythonOperator(
    task_id='etl_to_s3',
    python_callable=etl_to_s3,
    provide_context=True,
    dag=dag,
)

etl_to_s3_task
