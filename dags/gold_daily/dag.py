from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

AWS_CONN_ID = "aws"
RDS_SECRET = Variable.get("RDS_SECRET")

# SQL file names
QC_QUERY = "qc.sql"
PGX_REPORT_QUERY = "pgx_report.sql"
ILLUMINA_SEC = "staging_illumina_sec.sql"
MGI_SEC = "staging_mgi_sec.sql"
ONT_SEC = "staging_ont_sec.sql"
SEQ = "staging_seq.sql"
SIMBIOX = "staging_simbiox_biosamples_patients.sql"
SIMBIOX_REPORT_FIX_PROGRESS = "staging_report_fix_simbiox.sql"
SKI_FIX_ID_REPO = "staging_fix_ski_id_repo.sql"
SIMBIOX_TRANSFER_STAG = "staging_simbiox_transfer.sql"
SIMBIOX_TRANSFER_REPORT = "simbiox_transfer.sql"

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'gold_daily',
    default_args=default_args,
    description='ETL pipeline for scheduling daily queries for gold tables',
    schedule_interval="30 1 * * *",  # 1:30 AM everyday
    catchup=False,
)


def load_query(folder, filename):
    path = os.path.join("dags", "repo", "dags", "include", folder, filename)
    with open(path) as f:
        return f.read()


# Load SQL queries
qc_query = load_query("gold_query", QC_QUERY)
pgx_report_query = load_query("gold_query", PGX_REPORT_QUERY)
staging_illumina_sec_query = load_query("staging_query", ILLUMINA_SEC)
staging_mgi_sec_query = load_query("staging_query", MGI_SEC)
staging_ont_sec_query = load_query("staging_query", ONT_SEC)
staging_seq_query = load_query("staging_query", SEQ)
staging_simbiox_query = load_query("staging_query", SIMBIOX)
staging_ski_fix_id_repo_query = load_query("staging_query", SKI_FIX_ID_REPO)
staging_simbiox_report_fix_progress_query = load_query(
    "staging_query", SIMBIOX_REPORT_FIX_PROGRESS)
simbiox_transfer_stag_query = load_query(
    "staging_query", SIMBIOX_TRANSFER_STAG)
simbiox_transfer_report_query = load_query(
    "gold_query", SIMBIOX_TRANSFER_REPORT)

with dag:
    # Create sensors with similar parameters using a loop.
    with TaskGroup('loader_sensors') as loader_sensors:
        sensor_configs = [
            {"task_id": "pgx_report", "external_dag_id": "pgx_report", "execution_delta": timedelta(
                hours=1, minutes=30), "allowed_states": ["success"]},
            {"task_id": "zlims_pl", "external_dag_id": "zlims-pl",
                "execution_delta": timedelta(hours=1, minutes=30), "allowed_states": ["success"]},
            {"task_id": "wfhv_pl", "external_dag_id": "wfhv-pl",
                "execution_delta": timedelta(hours=1, minutes=30), "allowed_states": ["success"]},
            {"task_id": "simbiox-patients", "external_dag_id": "simbiox-patients",
                "execution_delta": timedelta(hours=1, minutes=30), "allowed_states": ["success", "failed"]},
            {"task_id": "simbiox-biosamples", "external_dag_id": "simbiox-biosamples",
                "execution_delta": timedelta(minutes=30), "allowed_states": ["success", "failed"]},
            {"task_id": "simbiox_tables", "external_dag_id": "simbiox_tables",
                "execution_delta": timedelta(minutes=90), "allowed_states": ["success", "failed"]},
            {"task_id": "regina-demography", "external_dag_id": "regina-demography",
                "execution_delta": timedelta(hours=1, minutes=30), "allowed_states": ["success", "failed"]},
            {"task_id": "phenovar", "external_dag_id": "phenovar", "execution_delta": timedelta(
                hours=1, minutes=30), "allowed_states": ["success"]},
            {"task_id": "mgi_pl", "external_dag_id": "mgi-pl",
                "execution_delta": timedelta(hours=1, minutes=30), "allowed_states": ["success"]},
            {"task_id": "illumina", "external_dag_id": "illumina", "execution_delta": timedelta(
                hours=1, minutes=30), "allowed_states": ["success"]},
            {"task_id": "ica_analysis", "external_dag_id": "ica-analysis", "external_task_id": "silver_transform_to_db",
                "execution_delta": timedelta(hours=1, minutes=30), "allowed_states": ["success"]},
            {"task_id": "ica_samples", "external_dag_id": "ica-samples", "external_task_id": "silver_transform_to_db",
                "execution_delta": timedelta(hours=1, minutes=30), "allowed_states": ["success"]},
        ]
        sensors = {}
        for cfg in sensor_configs:
            sensor = ExternalTaskSensor(
                task_id=cfg["task_id"],
                external_dag_id=cfg["external_dag_id"],
                execution_delta=cfg["execution_delta"],
                allowed_states=cfg["allowed_states"],
                timeout=60 * 60 * 2,  # 2 hours
                check_existence=True,
                exponential_backoff=True,
                **({"external_task_id": cfg["external_task_id"]} if "external_task_id" in cfg else {})
            )
            sensors[cfg["task_id"]] = sensor

    with TaskGroup('queries') as queries:
        # Create SQL query tasks
        conn_id = "bgsi-rds-mysql-prod-superset_dev"

        staging_ski_fix_id_repo_task = SQLExecuteQueryOperator(
            task_id="staging_ski_fix_id_repo",
            conn_id=conn_id,
            sql=staging_ski_fix_id_repo_query
        )
        staging_illumina_sec_task = SQLExecuteQueryOperator(
            task_id="staging_illumina_sec",
            conn_id=conn_id,
            sql=staging_illumina_sec_query
        )
        staging_mgi_sec_task = SQLExecuteQueryOperator(
            task_id="staging_mgi_sec",
            conn_id=conn_id,
            sql=staging_mgi_sec_query
        )
        staging_ont_sec_task = SQLExecuteQueryOperator(
            task_id="staging_ont_sec",
            conn_id=conn_id,
            sql=staging_ont_sec_query
        )
        staging_seq_task = SQLExecuteQueryOperator(
            task_id="staging_seq",
            conn_id=conn_id,
            sql=staging_seq_query
        )
        staging_simbiox_task = SQLExecuteQueryOperator(
            task_id="staging_simbiox",
            conn_id=conn_id,
            sql=staging_simbiox_query
        )
        staging_simbiox_report_fix_progress_task = SQLExecuteQueryOperator(
            task_id="staging_simbiox_report_fix_progress",
            conn_id=conn_id,
            sql=staging_simbiox_report_fix_progress_query
        )
        staging_simbiox_transfer_task = SQLExecuteQueryOperator(
            task_id="sstaging_simbiox_transfer",
            conn_id=conn_id,
            sql=simbiox_transfer_stag_query
        )
        gold_qc_task = SQLExecuteQueryOperator(
            task_id="qc",
            conn_id=conn_id,
            sql=qc_query
        )
        gold_simbiox_transfer_task = SQLExecuteQueryOperator(
            task_id="simbiox_transfer_report",
            conn_id=conn_id,
            sql=simbiox_transfer_report_query
        )
        gold_pgx_report_task = SQLExecuteQueryOperator(
            task_id="pgx_report",
            conn_id=conn_id,
            sql=pgx_report_query
        )
        staging_ski_fix_id_repo_task >> [staging_simbiox_task, staging_simbiox_transfer_task] # type: ignore
        [
            staging_mgi_sec_task,
            staging_ont_sec_task,
            staging_illumina_sec_task,
            staging_seq_task,
            staging_simbiox_task,
        ] >> gold_qc_task  # type: ignore
        
        [gold_qc_task, sensors["pgx_report"]] >> gold_pgx_report_task  # type: ignore
        staging_simbiox_transfer_task >> gold_simbiox_transfer_task  # type: ignore

    loader_sensors >> queries  # type: ignore
