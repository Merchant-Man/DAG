from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import re
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dateutil.parser import isoparse
from datetime import timezone
# Silver task
from utils.utils import fetch_and_dump, silver_transform_to_db

# ----------------------------
# Constants and Config
# ----------------------------

API_BASE = "https://api.aps4.sh.basespace.illumina.com/v2"
AWS_CONN_ID = "aws"
BSSH_CONN_ID = "bssh"
BSSH_APIKEY = Variable.get("BSSH_APIKEY1")
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
RDS_SECRET = Variable.get("RDS_SECRET")
# Updated OBJECT_PATH to match what silver_transform_to_db expects
OBJECT_PATH = "bssh/appsessions"
LOADER_QUERY_PATH = "illumina_appsession_loader.sql"

# ----------------------------
# Bronze: Fetch from API and Dump to S3
# ----------------------------
logger = LoggingMixin().log
def fetch_bclconvert_and_dump(aws_conn_id, bucket_name, object_path,
                               transform_func=None, **kwargs):
    curr_ds = kwargs["ds"]
    headers = {
        "Authorization": f"Bearer {Variable.get('BSSH_APIKEY1')}",
        "Content-Type": "application/json"
    }

    last_fetched_dt = datetime.strptime(curr_ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    limit = 25
    offset = 0
    all_rows = []
    full_sessions = []  # ✅ Store full appsession JSONs
    stop = False

    while not stop:
        resp = requests.get(
            f"{API_BASE}/appsessions?offset={offset}&limit={limit}&sortBy=DateCreated&sortDir=Desc",
            headers=headers
        )
        sessions = resp.json().get("Items", [])
        if not sessions:
            break

        for session in sessions:
            created_dt = isoparse(session["DateCreated"]).astimezone(timezone.utc)

            if created_dt <= last_fetched_dt:
                stop = True
                break

            if "BCLConvert" not in session.get("Name", ""):
                continue

            session_id = session["Id"]
            detail = requests.get(f"{API_BASE}/appsessions/{session_id}", headers=headers).json()

            full_sessions.append(detail)  # ✅ Add full session to the list

            properties = {
                item["Name"]: item.get("Content")
                for item in detail.get("Properties", {}).get("Items", [])
                if item.get("Name")
            }

            run_items = []
            for item in detail.get("Properties", {}).get("Items", []):
                if item.get("Name") == "Input.Runs":
                    run_items = item.get("RunItems", [])

            for run in run_items:
                all_rows.append({
                    "RowType": "Run",
                    "SessionId": session_id,
                    "SessionName": detail.get("Name"),
                    "DateCreated": detail.get("DateCreated"),
                    "DateModified": detail.get("DateModified"),
                    "ExecutionStatus": detail.get("ExecutionStatus"),
                    "ICA_Link": detail.get("HrefIcaAnalysis"),
                    "ICA_ProjectId": properties.get("ICA.ProjectId"),
                    "WorkflowReference": properties.get("ICA.WorkflowSessionUserReference"),
                    "RunId": run.get("Id"),
                    "RunName": run.get("Name"),
                    "PercentGtQ30": run.get("SequencingStats", {}).get("PercentGtQ30"),
                    "FlowcellBarcode": run.get("FlowcellBarcode"),
                    "ReagentBarcode": run.get("ReagentBarcode"),
                    "Status": run.get("Status"),
                    "ExperimentName": run.get("ExperimentName"),
                    "RunDateCreated": run.get("DateCreated")
                })

            logs_tail = next(
                (item.get("Content") for item in detail.get("Properties", {}).get("Items", [])
                 if item.get("Name") == "Logs.Tail"),
                ""
            )

            for line in logs_tail.splitlines():
                if "Computed yield for biosample" in line:
                    match = re.search(
                        r"Computed yield for biosample '([^']+)' \(Id: (\d+)\): (\d+) Bps", line)
                    if match:
                        biosample_name = match.group(1)
                        biosample_id = match.group(2)
                        yield_bps = match.group(3)

                        gen_sample_match = re.search(
                            rf"{biosample_name}.*?Generated new Sample: (\d+)",
                            logs_tail, re.DOTALL)
                        generated_sample_id = gen_sample_match.group(1) if gen_sample_match else None

                        all_rows.append({
                            "RowType": "BioSample",
                            "SessionId": session_id,
                            "SessionName": detail.get("Name"),
                            "DateCreated": detail.get("DateCreated"),
                            "RunName": run.get("Name"),
                            "ExperimentName": run.get("ExperimentName"),
                            "RunDateCreated": run.get("DateCreated"),
                            "BioSampleName": biosample_name,
                            "BioSampleId": biosample_id,
                            "ComputedYieldBps": yield_bps,
                            "GeneratedSampleId": generated_sample_id
                        })

        offset += limit

    # Transform and save CSV
    df = pd.DataFrame(all_rows)
    df = transform_func(df, curr_ds)

    buffer = io.StringIO()
    logger.info(f"✔ Total sessions fetched: {len(full_sessions)}")
    logger.info(f"✔ Total rows parsed: {len(all_rows)}")
    logger.info(f"✔ Final DataFrame shape: {df.shape}")
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3_path = f"bssh/appsessions/{curr_ds}/bclconvert_appsessions-{curr_ds}.csv"
    s3.load_string(buffer.getvalue(), s3_path, bucket_name=bucket_name, replace=True)
    print(f"✅ Saved to S3: {s3_path}")

# ----------------------------
# DAG Definition
# ----------------------------

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'bssh_bclconvert_appsessions',
    default_args=default_args,
    description='Fetch BCLConvert AppSessions from BSSH and load to S3 + RDS',
    schedule_interval=timedelta(days=1),
    catchup=False
)
with open(os.path.join("dags/repo/dags/include/loader", LOADER_QUERY_PATH)) as f:
    loader_query = f.read()

def transform_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:

    # Remove duplicates from the main DataFrame
    df = df.drop_duplicates()

    df = df.astype(str)
    return df

# Bronze task
fetch_and_dump_task = PythonOperator(
    task_id="bronze_fetch_bssh_bclconvert_appsessions",
    python_callable=fetch_bclconvert_and_dump,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": OBJECT_PATH,
        "transform_func": transform_data,
    },
    provide_context=True
)

silver_transform_to_db_task = PythonOperator(
    task_id="silver_transform_to_db",
    python_callable=silver_transform_to_db,
    dag=dag,
    op_kwargs={
        "aws_conn_id": AWS_CONN_ID,
        "bucket_name": S3_DWH_BRONZE,
        "object_path": f"{OBJECT_PATH}/{{{{ ds }}}}",
        "transform_func": transform_data,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}",
        "multi_files": True  # Enable multi-files mode to find files with date in name
    },
    templates_dict={"insert_query": loader_query},
    provide_context=True
)

# DAG flow
fetch_and_dump_task >> silver_transform_to_db_task
