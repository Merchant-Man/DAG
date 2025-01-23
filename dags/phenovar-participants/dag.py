from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import fetch_jwt_and_dump, silver_transform_to_db
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import os
from io import StringIO

AWS_CONN_ID="aws"
PHENOVAR_CONN_ID="phenovar-prod"
JWT_END_POINT="api/v1/institution/login"
DATA_END_POINT="api/v1/participants?perpage=20000"
JWT_PAYLOAD={
    "email": Variable.get("PHENOVAR_EMAIL"),
    "password": Variable.get("PHENOVAR_PASSWORD")
    }
OBJECT_PATH = "AF/phenovar/participants" # SHOULD CHANGE TODO
# OBJECT_PATH = "phenovar/demography" # SHOULD CHANGE TODO
S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "phenovar_particip_loader.sql"


default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "phenovar-participants",
    default_args=default_args,
    description="ETL pipeline for Phenovar participants data using Phenovar participants API",
    schedule_interval=timedelta(days=1),
    catchup=False
)

def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    df['gender'].replace({'female': 'FEMALE', 'male': 'MALE'}, inplace=True)
    df.rename(columns={'id': 'id_subject', 'created_at': 'creation_date',
    'updated_at': 'updation_date', 'gender': 'sex', 'nik': 'encrypt_nik', 'full_name': 'encrypt_full_name', 'dob': 'encrypt_birth_date'}, inplace=True)
        
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    df = df[["id_subject", "encrypt_full_name", "encrypt_nik", "encrypt_birth_date", "sex", "source", "province", "district", "created_at", "updated_at", "creation_date", "updation_date"]]

    # Even we remove duplicates, API might contain duplicate records for an id_subject
    # So, we will keep the latest record
    df['updation_date'] = pd.to_datetime(df['updation_date'])
    df = df.sort_values('updation_date').groupby('id_subject').tail(1)

    df['updation_date'] = df["updation_date"].dt.strftime('%Y-%m-%d %H:%M:%S').astype('str')

    # Need to fillna so that the mysql connector can insert the data.
    values = {
        "id_subject": "", "encrypt_full_name": "", "encrypt_nik": "", "encrypt_birth_date": "", "sex": "", "source": "", "province": "", "district": "", "creation_date": "", "updation_date": "" 
    }
    df.fillna(value=values, inplace=True)
    return df

# In local should be: with open(os.path.join("dags/include/loader", LOADER_QEURY)) as f:
with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()


bronze_fetch_jwt_and_dump_data_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_dump_data",
    python_callable=fetch_jwt_and_dump,
    dag=dag,
    op_args=[PHENOVAR_CONN_ID, DATA_END_POINT, JWT_END_POINT, AWS_CONN_ID, S3_DWH_BRONZE, OBJECT_PATH, {}, JWT_PAYLOAD, True], # conn_id, data_end_point, jwt_end_point, aws_conn_id, s3_bucket, data_payload, jwt_payload, is_using_cookie, transform_func
    op_kwargs={
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
    }
)

silver_transform_to_db_task = PythonOperator(
    task_id="silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_args=[AWS_CONN_ID, S3_DWH_BRONZE, OBJECT_PATH, transform_data, RDS_SECRET, loader_query], # aws_conn_id, s3_bucket, object_path, loader_query
    op_kwargs={
        "curr_ds": "{{ ds }}"
    }
)

bronze_fetch_jwt_and_dump_data_task >> silver_transform_to_db_task