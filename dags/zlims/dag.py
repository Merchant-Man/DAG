from datetime import datetime, timedelta
import os
from utils.zlims_transform import transform_samples_data
from utils.mgi_transform import ztron_transform_analysis_data
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils.utils import silver_transform_to_db

AWS_CONN_ID = "aws"
SAMPLES_OBJECT_PATH = "zlims/samples"
ANALYSIS_OBJECT_PATH = "ztron_pro/analysis"
S3_DWH_BRONZE = "bgsi-data-dwh-bronze"
RDS_SECRET = Variable.get("RDS_SECRET")
SAMPLES_LOADER_QUERY = "zlims_samples_loader.sql"
ANALYSIS_LOADER_QUERY = "ztronpro_analysis_loader.sql"

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'zlims-pl',
    default_args=default_args,
    description='ETL pipeline for fetching ZLims PL samples, and Analysis from Nextflow pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False
)

with open(os.path.join("dags/repo/dags/include/loader", SAMPLES_LOADER_QUERY)) as f:
    samples_loader_query = f.read()

with open(os.path.join("dags/repo/dags/include/loader", ANALYSIS_LOADER_QUERY)) as f:
    analysis_loader_query = f.read()

samples_silver_transform_to_db_task = PythonOperator(
    task_id="samples_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": SAMPLES_OBJECT_PATH,
        "transform_func": transform_samples_data,
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": samples_loader_query},
    provide_context=True
)

analysis_silver_transform_to_db_task = PythonOperator(
    task_id="analysis_silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": ANALYSIS_OBJECT_PATH,
        "transform_func": ztron_transform_analysis_data,
        "db_secret_url": RDS_SECRET,
        "multi_files": True,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": analysis_loader_query},
    provide_context=True
)
