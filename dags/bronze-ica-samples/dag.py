from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
ICA_APIKEY=Variable.get("ICA_APIKEY")
ICA_REGION=Variable.get("ICA_REGION")

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bronze-ica-samples',
    default_args=default_args,
    description='ETL pipeline using a public API',
    schedule_interval=timedelta(days=1),
)


def extract_transform_data(**kwargs):
    api_response = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_api')

    if 'items' in api_response:
        data_value = api_response['items']
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
    s3_key = f'ica/samples/{data_interval_start.isoformat()}.csv'
    
    # Use S3Hook to upload the data to S3
    s3 = S3Hook(aws_conn_id='aws')
    s3.load_string(
        string_data=transformed_data,
        key=s3_key,
        bucket_name=S3_DWH_BRONZE,  # replace with your S3 bucket name
        replace=True
    )


fetch_data = SimpleHttpOperator(
    task_id='fetch_data_from_api',
    method='GET',
    http_conn_id='ica',
    endpoint=f'/ica/rest/api/samples?region={ICA_REGION}&pageOffset=0&pageSize=10000&sort=timeCreated%20desc',
    headers={
        "accept": "application/vnd.illumina.v3+json",
        "X-API-Key": ICA_APIKEY
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

fetch_data >> transform_data >> upload_to_s3
