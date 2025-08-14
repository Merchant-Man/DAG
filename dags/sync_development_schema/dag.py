from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from datetime import datetime
from utils.sync_schema import get_production_tables, sync_schema

RDS_SECRET = Variable.get("RDS_SECRET")
RDS_DEVELOPMENT_SCHEMA = Variable.get("dwh_development_schema")
RDS_PRODUCTION_SCHEMA = Variable.get("dwh_production_schema")

engine = create_engine(RDS_SECRET)

default_args = {
    "owner": "bgsi-data",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 12),
    "email_on_failure": True, 
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id = "sync_development_schema",
    description = "Sync development schema with the latest changes",
    default_args = default_args,
    schedule_interval = "* 17 * * 7", # at 17:00 UTC every day (which is 00:00 WIB)
    catchup = False
)

with dag:
    get_production_tables_task = PythonOperator(
        task_id="get_production_tables_task",
        python_callable=get_production_tables,
        op_kwargs={
            "production_schema": RDS_PRODUCTION_SCHEMA,
            "engine": engine
        },
        provide_context=True
    )
    
    sync_schema_task = PythonOperator(
        task_id="sync_schema_task",
        python_callable=sync_schema,
        op_kwargs={
            "staging_schema": RDS_DEVELOPMENT_SCHEMA,
            "production_schema": RDS_PRODUCTION_SCHEMA,
            "engine": engine
        },
        provide_context=True
    )

    get_production_tables_task >> sync_schema_task #type: ignore
