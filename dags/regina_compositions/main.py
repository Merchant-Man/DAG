from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.regina_transform import fetch_regina_log_to_db, get_token_function
from airflow.operators.python import PythonOperator

AWS_CONN_ID="aws"
REGINA_CONN_ID="regina-prod"
JWT_END_POINT="api/v1/user/login"
DATA_END_POINT="api/v1/registry/logs/data/filter"
OBJECT_PATH = "regina/compositions"
S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")

JWT_PAYLOAD={
    "username": Variable.get("REGINA_USERNAME"),
    "password": Variable.get("REGINA_PASSWORD")
    }

default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "regina-compositions",
    default_args=default_args,
    description="ETL pipeline for RegINA clinical data using RegINA AQL API",
    schedule_interval="0 0 * * *",
    catchup=True,
    max_active_runs=5 # for backfill purposes
)

GET_ID_QUERY = "SELECT id_subject FROM regina_demography ORDER BY id_subject"

get_composition_log_task = PythonOperator(
    task_id="get_composition_log",
    python_callable=fetch_regina_log_to_db,
    dag=dag,
    op_kwargs={
        "api_conn_id": REGINA_CONN_ID,
        "data_end_point": DATA_END_POINT,
        "jwt_end_point": JWT_END_POINT,
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "db_secret_url": RDS_SECRET,
        "object_path": OBJECT_PATH,
        "jwt_payload": JWT_PAYLOAD,
        "jwt_headers": {"Content-Type": "application/json"},
        "get_token_function": get_token_function,
        "curr_ds": "{{ ds }}",
    },
    provide_context=True
)
