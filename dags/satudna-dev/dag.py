import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import boto3
from datetime import datetime, timedelta

RDS_SECRET = Variable.get("RDS_SECRET")
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
            WITH combined_analysis AS (
                SELECT DISTINCT 
                    id_repository,
                    CASE 
                        WHEN run_status = 'SUCCEEDED' THEN 'SUCCEEDED'
                        ELSE 'FAILED'
                    END as analysis_status
                FROM (
                    SELECT id_repository, run_status FROM superset_dev.ica_analysis_latest
                    UNION ALL
                    SELECT id_repository, run_status FROM superset_dev.mgi_analysis_latest
                ) all_analysis
            ),
            combined_qc AS (
                SELECT 
                    id_repository,
                    CASE 
                        WHEN at_least_10x >= 80 
                        AND median_coverage >= 30 THEN TRUE
                        ELSE FALSE
                    END as qc_pass
                FROM (
                    SELECT id_repository, at_least_10x, median_coverage 
                    FROM superset_dev.mgi_qc_latest
                    UNION ALL
                    SELECT id_repository, at_least_10x, median_coverage
                    FROM superset_dev.illumina_qc_latest
                ) all_qc
            )
            SELECT 
                a.id_repository,
                a.analysis_status as analysis_secondary,
                COALESCE(q.qc_pass, FALSE) as qc_pass
            FROM combined_analysis a
            LEFT JOIN combined_qc q ON a.id_repository = q.id_repository
        """
        df = pd.read_sql(query, engine)
        
        context['task_instance'].xcom_push(
            key='rds_data', 
            value=df.to_dict(orient='records')
        )
        print(f"Fetched {len(df)} records from RDS")
    except Exception as e:
        print(f"Error fetching from RDS: {e}")
        raise

def load_to_dynamo(**context):
    try:
        records = context['task_instance'].xcom_pull(
            task_ids='fetch_rds_data',
            key='rds_data'
        )
        
        dynamodb = boto3.resource('dynamodb')
        
        existing_tables = dynamodb.meta.client.list_tables()['TableNames']
        if 'satudna-dev' not in existing_tables:
            table = dynamodb.create_table(
                TableName='satudna-dev',
                KeySchema=[
                    {
                        'AttributeName': 'id_subject',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id_subject',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            table.meta.client.get_waiter('table_exists').wait(TableName='satudna-dev')
            print("Created DynamoDB table 'satudna-dev'")
        
        table = dynamodb.Table('satudna-dev')
        with table.batch_writer() as batch:
            for record in records:
                item = {
                    'id_subject': str(record['id_repository']),
                    'analysis_secondary': str(record['analysis_secondary']),
                    'qc_pass': str(record['qc_pass'])
                }
                batch.put_item(Item=item)
        
        print(f"Loaded {len(records)} items to DynamoDB")
    except Exception as e:
        print(f"Error loading to DynamoDB: {e}")
        raise

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