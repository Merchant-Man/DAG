from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.utils import fetch_and_dump, silver_transform_to_db
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from requests.exceptions import ConnectionError
import tenacity
import os
from io import StringIO

AWS_CONN_ID="aws"
SIMBIOX_CONN_ID="simbiox-prod"
DATA_END_POINT="index.php/api/Table/get/tbl_data_patients"
OBJECT_PATH = "AF/simbiox/patients" # SHOULD CHANGE TODO
# OBJECT_PATH = "simbiox/patients" # SHOULD CHANGE TODO
S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
SIMBIOX_APIKEY=Variable.get("SIMBIOX_APIKEY")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "simbiox_patients_loader.sql"


default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    df['id_subject'] = df['nomor_mr']
    df['id_patient'] = df['id']
        
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    df.rename(columns={"dateofbirth": "date_of_birth"}, inplace=True)

    df = df[["id_patient","id_mpi", "id_subject", "sex",
    "date_of_birth", "id_biobank", "created_at", "updated_at"]]
    
    # Need to fillna so that the mysql connector can insert the data.
    values = {
        "id_mpi": "", "id_subject": "", "sex": "", "date_of_birth": "", "sex": "", "id_biobank": ""
    }
    df.fillna(value=values, inplace=True)
    return df
    
dag = DAG(
    "simbiox-patients",
    default_args=default_args,
    description="ETL pipeline for Simbiox Patients API data (tbl_data_patients)",
    schedule_interval=timedelta(days=1),
    catchup=False
)

# In local should be with open(os.path.join("dags/include/loader", LOADER_QEURY)) as f:
with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()


# tenacy max({min}, min(2^{try} * 1, {max})
# will do 32 secs 5 times and then 64, 128, 256, 512, 1024.
retry_args = dict(
    wait=tenacity.wait_exponential(multiplier=1, min=32, max=1024),
    stop=tenacity.stop_after_attempt(10),
    retry=ConnectionError
    )

fetch_and_dump_task = PythonOperator(
    task_id="bronze_fetch_and_dump_data",
    python_callable=fetch_and_dump,
    dag=dag,
    op_args=[SIMBIOX_CONN_ID, DATA_END_POINT, AWS_CONN_ID, S3_DWH_BRONZE, OBJECT_PATH, {"api-key":SIMBIOX_APIKEY}, {}, retry_args],
    provide_context=True
)

silver_transform_to_db_task = PythonOperator(
    task_id="silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_args=[AWS_CONN_ID, S3_DWH_BRONZE, OBJECT_PATH, transform_data, RDS_SECRET, loader_query],
    op_kwargs={
        "curr_ds": "{{ ds }}"
    },
    provide_context=True
)


    
fetch_and_dump_task >> silver_transform_to_db_task


    
    
    