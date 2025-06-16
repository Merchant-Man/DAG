from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
DYNAMODB_TABLE = Variable.get("DYNAMODB_TABLE")

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bronze-dynamodb-fix-id_repository',
    default_args=default_args,
    description='ETL pipeline using a public API',
    schedule_interval=timedelta(days=1),
)


def fetch_data(**kwargs):
    table = DynamoDBHook(
        aws_conn_id='aws', resource_type='dynamodb').get_conn().Table(DYNAMODB_TABLE)  # type: ignore

    # Get today's date in the same format as your DynamoDB timestamp (up to the day)
    today = datetime.now(timezone.utc).strftime('%Y:%m:%d 00:00:00')

    # Filter items with timestamps >= today
    response = table.scan(
        FilterExpression=Attr('timestamps').gte(today)
    )
    data_items = response.get('Items', [])

    kwargs['ti'].xcom_push(key='dynamodb_data', value=data_items)


def extract_transform_data(**kwargs):
    # Pull data from XCom
    dynamodb_data = kwargs['ti'].xcom_pull(
        task_ids='fetch_data', key='dynamodb_data')
    if not dynamodb_data:
        raise ValueError("No data retrieved from DynamoDB.")

    # Transform data into a DataFrame
    df = pd.DataFrame(dynamodb_data)

    # Split by 'fix_type'
    grouped = df.groupby('fix_type')

    csv_outputs = {}
    for fix_type, group in grouped:
        csv_buffer = StringIO()
        group.to_csv(csv_buffer, index=False)
        csv_outputs[fix_type] = csv_buffer.getvalue()

    # Push each CSV string to XCom separately
    for fix_type, csv_str in csv_outputs.items():
        kwargs['ti'].xcom_push(key=f'transformed_data_{fix_type}', value=csv_str)

def load_data(**kwargs):
    ti = kwargs['ti']
    data_interval_start = ti.get_dagrun().data_interval_start

    # Define fix_type -> folder mapping
    fix_types = ['id_repository', 'id_library']

    s3 = S3Hook(aws_conn_id='aws')

    for fix_type in fix_types:
        # Pull each transformed CSV from XCom
        transformed_data = ti.xcom_pull(
            task_ids='transform_data', key=f'transformed_data_{fix_type}'
        )

        if not transformed_data:
            continue  # Skip if data doesn't exist for this fix_type

        # Define S3 key
        s3_key = f'dynamodb/fix/{fix_type}/{data_interval_start.isoformat()}.csv'

        # Upload to S3
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
    op_kwargs={
        "curr_ds": "{{ ds }}"
    },
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
