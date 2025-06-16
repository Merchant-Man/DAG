# from datetime import datetime, timedelta
# from io import StringIO
# import pandas as pd
# from airflow import DAG
# from airflow.models import Variable
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
# from utils.fix_transform import fetch_data, extract_transform_data, load_data

# AWS_CONN_ID = "aws"
# S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
# DYNAMODB_TABLE = Variable.get("DYNAMODB_TABLE")
# FIX_OBJECT_PATH = "dynamodb/fix"

# default_args = {
#     'owner': 'bgsi-data',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 6, 14),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
# }

# dag = DAG(
#     'dynamodb-fix',
#     default_args=default_args,
#     description='ETL pipeline using a public API',
#     schedule_interval=timedelta(days=1),
# )

# # Define tasks
# fetch_data_task = PythonOperator(
#     task_id='fetch_data',
#     python_callable=fetch_data,
#     provide_context=True,
#     dag=dag,
#     op_kwargs={
#         "aws_conn_id": AWS_CONN_ID,
#         "dynamodb_table": DYNAMODB_TABLE,
#         "curr_ds": "{{ ds }}"
#     },
# )

# transform_data_task = PythonOperator(
#     task_id='transform_data',
#     python_callable=extract_transform_data,
#     provide_context=True,
#     dag=dag,
# )

# upload_to_s3_task = PythonOperator(
#     task_id='load_data',
#     python_callable=load_data,
#     provide_context=True,
#     dag=dag,
#     op_kwargs={
#         "aws_conn_id": AWS_CONN_ID,
#         "bronze_bucket": S3_DWH_BRONZE,
#         "bronze_object_path": FIX_OBJECT_PATH,
#         "curr_ds": "{{ ds }}"
#     },
# )

# # Task dependencies
# fetch_data_task >> transform_data_task >> upload_to_s3_task
