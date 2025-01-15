from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook

S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
DYNAMODB_TABLE='bgsi-upload-ont'

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bronze-wfhv-samples',
    default_args=default_args,
    description='ETL pipeline using a public API',
    schedule_interval=timedelta(days=1),
)


def fetch_data(**kwargs):
    table = DynamoDBHook(aws_conn_id='aws', resource_type='dynamodb').conn.Table(DYNAMODB_TABLE)
    
    # Fetch data count from DynamoDB
    response = table.scan(Select='ALL_ATTRIBUTES')
    data_items = response.get('Items', [])
    
    # Push data to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='dynamodb_data', value=data_items)

def extract_transform_data(**kwargs):
    # Pull data from XCom
    dynamodb_data = kwargs['ti'].xcom_pull(task_ids='fetch_data', key='dynamodb_data')
    if not dynamodb_data:
        raise ValueError("No data retrieved from DynamoDB.")
    
    # Transform data into a DataFrame
    df = pd.DataFrame(dynamodb_data)
    
    # Convert DataFrame to CSV format
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Return the CSV data as string
    return csv_buffer.getvalue()

def load_data(**kwargs):
    # Retrieve the transformed CSV from XCom
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    # Generate S3 key using data_interval_start
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'wfhv/samples/{data_interval_start.isoformat()}.csv'
    
    # Use S3Hook to upload the data to S3
    s3 = S3Hook(aws_conn_id='aws')
    s3.load_string(
        string_data=transformed_data,
        key=s3_key,
        bucket_name=S3_DWH_BRONZE,
        replace=True
    )

# Define tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=extract_transform_data,
    provide_context=True,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Task dependencies
fetch_data_task >> transform_data_task >> upload_to_s3_task