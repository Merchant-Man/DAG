from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import fetch_jwt_and_dump, silver_transform_to_db
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import os
from io import StringIO

AWS_CONN_ID="aws"
REGINA_CONN_ID="regina-prod"
JWT_END_POINT="api/v1/user/login"
DATA_END_POINT="api/v2/registry/demography/list?limit=20000"
JWT_PAYLOAD={
    "username": Variable.get("REGINA_USERNAME"),
    "password": Variable.get("REGINA_PASSWORD")
    }
OBJECT_PATH = "AF/regina/demography" # SHOULD CHANGE TODO
# OBJECT_PATH = "regina/demography" # SHOULD CHANGE TODO
S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "regina_demog_loader.sql"


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
    "regina-demography",
    default_args=default_args,
    description="ETL pipeline for RegINA demography data using RegINA demography API",
    schedule_interval=timedelta(days=1),
    catchup=False
)

def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()
    
    df["id_subject"] = df["ehr_id"]
    df["sex"] = df["gender"].replace({"Perempuan": "FEMALE", "Laki-laki": "MALE"})
    df["age"] = df["age"].astype("int64")
    
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts
    

    df = df[["id_subject", "age", "sex", "created_at", "updated_at", "creationDate"]]

    # Even we remove duplicates, RegINA API might contain duplicate records for an id_subject
    # So, we will keep the latest record
    df['creationDate'] = pd.to_datetime(df['creationDate'])
    df = df.sort_values('creationDate').groupby('id_subject').tail(1)

    return df


with open(os.path.join("dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()

bronze_fetch_jwt_and_data_task = PythonOperator(
    task_id="bronze_fetch_jwt_and_data",
    python_callable=fetch_jwt_and_dump,
    dag=dag,
    op_args=[REGINA_CONN_ID, DATA_END_POINT, JWT_END_POINT, AWS_CONN_ID, S3_DWH_BRONZE, OBJECT_PATH, {}, JWT_PAYLOAD, False, "records"],
    op_kwargs={
        "curr_ds": "{{ ds }}",
        "prev_ds": "{{ prev_ds }}"
    }
)

silver_transform_to_db_task = PythonOperator(
    task_id="silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_args=[AWS_CONN_ID, S3_DWH_BRONZE, OBJECT_PATH, transform_data, RDS_SECRET, loader_query], 
    op_kwargs={
        "curr_ds": "{{ ds }}"
    }
)

bronze_fetch_jwt_and_data_task >> silver_transform_to_db_task