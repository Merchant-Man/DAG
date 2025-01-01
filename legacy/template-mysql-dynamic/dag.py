import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from sqlalchemy import create_engine
from datetime import datetime, timedelta
import io

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER = Variable.get("S3_DWH_SILVER")

keys = [
    'ica/analysis/latest.csv',
    'illumina/qc/latest.csv',
    'mgi/qc/latest.csv',
    'phenovar/participants/latest.csv',
    'regina/demography/latest.csv',
    'simbiox/biosample/latest.csv',
    'simbiox/patients/latest.csv'

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
    'loader_rds_superset_dev',
    default_args=default_args,
    description='ETL pipeline to merge CSV files from S3 to RDS',
    schedule_interval=timedelta(days=1),
)


def fetch_data_from_s3(**kwargs):
    s3 = S3Hook(aws_conn_id='aws')
    key = kwargs['key']
    try:
        csv_obj = s3.get_key(key=key, bucket_name=S3_DWH_SILVER)
        df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
        kwargs['ti'].xcom_push(key=key, value=df.to_dict(orient='records'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Key {key} not found in S3, skipping.")
            return
        else:
            raise e

def upload_to_rds(**kwargs):
    key = kwargs['key']
    engine = create_engine(RDS_SECRET)
    df_records = kwargs['ti'].xcom_pull(task_ids=f'fetch_data_task_{key}', key=key)
    if df_records:
        df = pd.DataFrame(df_records)
        table_name = key.replace('/', '_').replace('.csv', '')
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
    else:
        print(f"No data fetched for {key}, skipping upload.")

for key in keys:
    task = key.replace('/', '_').replace('.csv', '')

    fetch_data_task = PythonOperator(
        task_id=f'fetch_data_task_{task}',
        python_callable=fetch_data_from_s3,
        op_kwargs={'key': key},
        provide_context=True,
        dag=dag,
    )

    upload_to_rds_task = PythonOperator(
        task_id=f'upload_to_rds_task_{task}',
        python_callable=upload_to_rds,
        op_kwargs={'key': key},
        provide_context=True,
        dag=dag,
    )

    fetch_data_task >> upload_to_rds_task
