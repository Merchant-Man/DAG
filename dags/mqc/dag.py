from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.mqc import extract_incomplete_qc, fetch_qc_files
from airflow.models import Variable

AWS_CONN_ID = "aws"
# S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
S3_BUCKET = "bgsi-data-dev"
RDS_SECRET = Variable.get("RDS_SECRET")
S3_DRAGEN_QC = "bgsi-data-dragen-qc"
S3_CITUS_QC = "bgsi-data-citus-qc"
OBJECT_PATH = "RT/mqc/samplesheets"

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mqc-pl',
    default_args=default_args,
    description='Extracts incomplete or missing QC data from gold_qc table',
    schedule_interval=timedelta(days=1),
    catchup=False
)

with dag:
    sync_qc_files_to_mqc_bucket_task = PythonOperator(
        task_id='sync_qc_files_to_mqc_bucket',
        python_callable=fetch_qc_files,
        op_kwargs={
            "aws_conn_id": AWS_CONN_ID,
            "curr_ds": "{{ ds }}"
        },
        provide_context=True
    )

    create_pl_mqc_samplesheets_task = PythonOperator(
        task_id='create_pl_mqc_samplesheets',
        python_callable=extract_incomplete_qc,
        op_kwargs={
            "aws_conn_id": AWS_CONN_ID,
            "bucket_name": S3_BUCKET,
            "dragen_bucket": S3_DRAGEN_QC,
            "citus_bucket": S3_CITUS_QC,
            "object_path": OBJECT_PATH,
            "db_uri": RDS_SECRET,
            "curr_ds": "{{ ds }}"
        },
        provide_context=True
    )

sync_qc_files_to_mqc_bucket_task >> create_pl_mqc_samplesheets_task