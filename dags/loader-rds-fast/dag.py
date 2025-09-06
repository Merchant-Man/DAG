import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert
from datetime import datetime, timedelta
import io

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER = Variable.get("S3_DWH_SILVER")

keys = [
    'dynamodb/fix/id_repository/latest.csv'
]

RDS_SECRET = Variable.get("RDS_SECRET")
engine = create_engine(RDS_SECRET)

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
    'loader-rds-fast',
    default_args=default_args,
    description='ETL pipeline to merge CSV files from S3 to RDS',
    schedule_interval=timedelta(days=1),
    catchup=False
)


def fetch_data_from_s3(**kwargs):
    s3 = S3Hook(aws_conn_id='aws')
    key = kwargs['key']
    try:
        csv_obj = s3.get_key(key=key, bucket_name=S3_DWH_SILVER)
        df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))

        kwargs['ti'].xcom_push(key=key, value=df.to_dict(orient='records'))
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e


def upload_to_rds(**kwargs):
    key = kwargs['key']
    task = kwargs['task']

    engine = create_engine(RDS_SECRET)
    df_records = kwargs['ti'].xcom_pull(
        task_ids=f'fetch_data_task_{task}', key=key)

    df = pd.DataFrame(df_records)
    table_name = key.replace('/', '_').replace('.csv', '')

    if not df.empty:
        with engine.connect() as conn:
            df.to_sql(table_name, con=conn, if_exists='replace', index=False)
            print(f"Table {table_name} replaced.")


for key in keys:
    task = key.replace('/', '_').replace('.csv', '')

    fetch_data_task = PythonOperator(
        task_id=f'fetch_data_task_{task}',
        python_callable=fetch_data_from_s3,
        op_kwargs={'key': key, 'task': task},
        provide_context=True,
        dag=dag,
    )

    upload_to_rds_task = PythonOperator(
        task_id=f'upload_to_rds_task_{task}',
        python_callable=upload_to_rds,
        op_kwargs={'key': key, 'task': task},
        provide_context=True,
        dag=dag,
    )

    fetch_data_task >> upload_to_rds_task # type: ignore
