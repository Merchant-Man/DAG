from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils.sequencing_sheets import dump_sequencing_sheets_to_s3_and_db


AWS_CONN_ID = "aws"
SEQ_OBJECT_PATH = "sheets/sequencing"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")

default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 18),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "sheets_daily",
    default_args=default_args,
    description="ETL pipeline for getting Google Sheets data",
    schedule_interval=timedelta(days=1),
    catchup=False
)


sequencing_data_dump_task = PythonOperator(
    task_id="sequencing_data_dump",
    python_callable=dump_sequencing_sheets_to_s3_and_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": SEQ_OBJECT_PATH,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}"
    },
    provide_context=True
)
