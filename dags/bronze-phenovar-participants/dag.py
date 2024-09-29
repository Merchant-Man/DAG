from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

PHENOVAR_EMAIL=Variable.get("PHENOVAR_EMAIL")
PHENOVAR_PASSWORD=Variable.get("PHENOVAR_PASSWORD")
S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")

default_args = {
    'owner': 'bgsi_data',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bronze-phenovar-participants',
    default_args=default_args,
    description='ETL pipeline using an API',
    schedule_interval=timedelta(days=5),
)

def extract_transform_data(**kwargs):
    api_response = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_api')
    
    if 'data' in api_response:
        data_value = api_response['data']
    else:
        raise ValueError("Response does not contain 'data' key.")
    
    # Transform API response into a DataFrame
    df = pd.DataFrame(data_value)
    
    # Convert DataFrame to CSV format
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Return the CSV data as string
    return csv_buffer.getvalue()


def load_data(**kwargs):
    # Retrieve the transformed CSV from XCom
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'phenovar/participants/{data_interval_start.isoformat()}.csv'
    
    # Use S3Hook to upload the data to S3
    s3 = S3Hook(aws_conn_id='aws')
    s3.load_string(
        string_data=transformed_data,
        key=s3_key,
        bucket_name=S3_DWH_BRONZE,  # replace with your S3 bucket name
        replace=True
    )

def set_headers(**context):
    auth_headers = context['ti'].xcom_pull(task_ids='fetch_jwt_token')
    return auth_headers

# Task to retrieve JWT token
fetch_jwt_token = SimpleHttpOperator(
    task_id='fetch_jwt_token',
    method='POST',
    http_conn_id='phenovar-prod',  # Replace with your HTTP connection ID
    endpoint='api/v1/institution/login',  # Adjust endpoint based on your auth API
    headers={"Content-Type": "application/json"},
    data=json.dumps({
        "email": PHENOVAR_EMAIL,
        "password": PHENOVAR_PASSWORD
    }),
    response_filter=lambda response: {
        "Authorization": f"Bearer {response.json().get('data', {}).get('access_token')}",
        "Cookie": response.headers.get('Set-Cookie')
    }, 
    log_response=True,
    dag=dag,
)

fetch_data = SimpleHttpOperator(
    task_id='fetch_data_from_api',
    method='GET',
    http_conn_id='phenovar-prod',
    endpoint='api/v1/participants?perpage=10000',
    headers={
        "Authorization": "{{ task_instance.xcom_pull(task_ids='fetch_jwt_token')['Authorization'] }}",
        "Cookie": "{{ task_instance.xcom_pull(task_ids='fetch_jwt_token')['Cookie'] }}"
    },
    response_filter=lambda response: response.json(),
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

fetch_jwt_token >> fetch_data >> transform_data >> upload_to_s3
