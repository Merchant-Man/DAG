import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from airflow.models import Connection
import boto3
from datetime import datetime, timedelta

RDS_SECRET = Variable.get("RDS_SECRET")
AWS_CONN_ID = "aws"
engine = create_engine(RDS_SECRET)

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'satudna-dev',
    default_args=default_args,
    description='Extract specific columns from RDS and load to DynamoDB for satudna',
    schedule_interval=timedelta(days=1),
)

def fetch_from_rds(**context):
    try:
        query = """
            SELECT
                id_repository,
                CASE
                    WHEN date_secondary IS NOT NULL THEN "SUCCEEDED"
                    ELSE "FAILED"
                END analysis_secondary,
                CASE
                    WHEN qc_category = "Pass" THEN TRUE
                    ELSE FALSE
                END qc_pass
            FROM
                gold_qc
        """
        df = pd.read_sql(query, engine)
        # Drop duplicates if any still exist after the SQL-level deduplication
        df = df.drop_duplicates(subset=['id_repository'])
        context['task_instance'].xcom_push(
            key='rds_data', 
            value=df.to_dict(orient='records')
        )
    except Exception as e:
        raise e

def load_to_dynamo(**context):
    try:
        records = context['task_instance'].xcom_pull(
            task_ids='fetch_rds_data',
            key='rds_data'
        )
        
        if not records:
            raise ValueError("No records retrieved from XCom")
        
        dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-3', 
                           aws_access_key_id=Connection.get_connection_from_secrets(
                               AWS_CONN_ID).login,
                           aws_secret_access_key=Connection.get_connection_from_secrets(AWS_CONN_ID).password)
        table = dynamodb.Table('satudna-dev')
        
        # Create a set to track processed IDs
        processed_ids = set()
        
        with table.batch_writer() as batch:
            for record in records:
                id_subject = str(record['id_repository'])
                # Skip if we've already processed this ID
                if id_subject in processed_ids:
                    continue
                    
                processed_ids.add(id_subject)
                item = {
                    'id_subject': id_subject,
                    'analysis_secondary': str(record['analysis_secondary']),
                    'qc_pass': str(record['qc_pass'])
                }
                batch.put_item(Item=item)
    except Exception as e:
        raise e

fetch_rds_data = PythonOperator(
    task_id='fetch_rds_data',
    python_callable=fetch_from_rds,
    provide_context=True,
    dag=dag,
)

load_dynamo_data = PythonOperator(
    task_id='load_dynamo_data',
    python_callable=load_to_dynamo,
    provide_context=True,
    dag=dag,
)

fetch_rds_data >> load_dynamo_data