from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


AWS_CONN_ID = "aws"
RDS_SECRET = Variable.get("RDS_SECRET")
QC_QUERY = "qc.sql"
PGX_REPORT_QUERY = "pgx_report.sql"
ILLUMINA_SEC="staging_illumina_sec.sql"
MGI_SEC="staging_mgi_sec.sql"
ONT_SEC="staging_ont_sec.sql"
SEQ="staging_seq.sql"
SIMBIOX="staging_simbiox_biosamples_patients.sql"

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
    schedule_interval="30 1 * * *",  # @1:30 AM everyday,
    catchup=False
)

with open(os.path.join("dags/repo/dags/include/gold_query", QC_QUERY)) as f:
    qc_query = f.read()

with open(os.path.join("dags/repo/dags/include/gold_query", PGX_REPORT_QUERY)) as f:
    pgx_report_query = f.read()

with open(os.path.join("dags/repo/dags/include/staging_query", ILLUMINA_SEC)) as f:
    staging_illumina_sec_query = f.read()

with open(os.path.join("dags/repo/dags/include/staging_query", MGI_SEC)) as f:
    staging_mgi_sec_query = f.read()

with open(os.path.join("dags/repo/dags/include/staging_query", ONT_SEC)) as f:
    staging_ont_sec_query = f.read()

with open(os.path.join("dags/repo/dags/include/staging_query", SEQ)) as f:
    staging_seq_query = f.read()

with open(os.path.join("dags/repo/dags/include/staging_query", SIMBIOX)) as f:
    staging_simbiox_query = f.read()

with dag:
    with TaskGroup('loader_sensors') as loader_sensors:
        # By default, each of the task will poke in the interval of 60 seconds based on the BaseSensorOperator
        # Currently, we need to manually defined each sensor and the timedelta for the waited task to be exactly matched the execution time of the external DAG
        pgx_report_pl = ExternalTaskSensor(
            task_id="pgx_report",
            external_dag_id="pgx_report",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success"]
        )

        zlims_pl = ExternalTaskSensor(
            task_id="zlims_pl",
            external_dag_id="zlims-pl",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success"]
        )

        wfhv_pl = ExternalTaskSensor(
            task_id="wfhv_pl",
            external_dag_id="wfhv-pl",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success"]
        )

        simbiox_patients = ExternalTaskSensor(
            task_id="simbiox-patients",
            external_dag_id="simbiox-patients",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success", "failed"] # failed is allowed since regina api is not stable.
        )

        simbiox_biosamples = ExternalTaskSensor(
            task_id="simbiox-biosamples",
            external_dag_id="simbiox-biosamples",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30),
            exponential_backoff=True,
            allowed_states=["success", "failed"] # failed is allowed since simbiox api is not stable.
        )

        regina_demography = ExternalTaskSensor(
            task_id="regina-demography",
            external_dag_id="regina-demography",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success", "failed"] # failed is allowed since regina api is not stable. 
        )

        phenovar_participants = ExternalTaskSensor(
            task_id="phenovar-participants",
            external_dag_id="phenovar-participants",
            external_task_id="silver_transform_to_db",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success"]
        )

        mgi_pl = ExternalTaskSensor(
            task_id="mgi_pl",
            external_dag_id="mgi-pl",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success"]
        )

        illumina = ExternalTaskSensor(
            task_id="illumina",
            external_dag_id="illumina",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success"]
        )

        ica_analysis = ExternalTaskSensor(
            task_id="ica_analysis",
            external_dag_id="ica-analysis",
            external_task_id="silver_transform_to_db",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success"]
        )

        ica_samples = ExternalTaskSensor(
            task_id="ica_samples",
            external_dag_id="ica-samples",
            external_task_id="silver_transform_to_db",
            check_existence=True,
            timeout=60*60*2,  # 2 hours
            execution_delta=timedelta(minutes=30, hours=1),
            exponential_backoff=True,
            allowed_states=["success"]
        )

    with TaskGroup('queries') as queries:
        staging_illumina_sec_task = SQLExecuteQueryOperator(
            task_id="staging_illumina_sec",
            conn_id="bgsi-rds-mysql-prod-superset_dev",
            sql=staging_illumina_sec_query
        )
        staging_mgi_sec_task = SQLExecuteQueryOperator(
            task_id="staging_mgi_sec",
            conn_id="bgsi-rds-mysql-prod-superset_dev",
            sql=staging_mgi_sec_query
        )
        staging_ont_sec_task = SQLExecuteQueryOperator(
            task_id="staging_ont_sec",
            conn_id="bgsi-rds-mysql-prod-superset_dev",
            sql=staging_ont_sec_query
        )
        staging_seq_task = SQLExecuteQueryOperator(
            task_id="staging_seq",
            conn_id="bgsi-rds-mysql-prod-superset_dev",
            sql=staging_seq_query
        )
        staging_simbiox_task = SQLExecuteQueryOperator(
            task_id="staging_simbiox",
            conn_id="bgsi-rds-mysql-prod-superset_dev",
            sql=staging_simbiox_query
        )
        gold_qc_task = SQLExecuteQueryOperator(
            task_id="qc",
            conn_id="bgsi-rds-mysql-prod-superset_dev",
            sql=qc_query
        ) 
        gold_pgx_report_task = SQLExecuteQueryOperator(
            task_id="pgx_report",
            conn_id="bgsi-rds-mysql-prod-superset_dev",
            sql=pgx_report_query
        ) 
        # If you want to create dependencies between queries
        # foo >> foo2
        [staging_mgi_sec_task, staging_ont_sec_task, staging_illumina_sec_task, staging_seq_task, staging_simbiox_task] >> gold_qc_task
        [gold_qc_task, pgx_report_pl] >> gold_pgx_report_task

# Please addd the source if applicable
loader_sensors >> queries
