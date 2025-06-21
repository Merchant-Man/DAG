import os
from io import StringIO
from typing import Tuple
import re
import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from botocore.client import BaseClient
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.utils import dict_csv_buf_transform, silver_transform_to_db
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

AWS_CONN_ID = "aws"
OBJECT_PATH = "pgx/report_logs"
INPUT_BUCKET_NAME = "nl-data-pgx-input"
OUTPUT_BUCKET_NAME = "nl-data-pgx-output"
OUTPUT_PATH_TEMPLATE = "production/{sample_id}/report_{report_type}/production.{sample_id}.{report_type}.data.json"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
SIMBIOX_APIKEY = Variable.get("SIMBIOX_APIKEY")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "staging_pgx_report_status.sql"

default_args = {
    "owner": "bgsi_data",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 22),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "pgx_report",
    default_args=default_args,
    description="ETL pipeline for getting the report of PGx data",
    schedule_interval=timedelta(days=1),
    catchup=False
)


def extract_info(pd_row: pd.DataFrame, col) -> Tuple[str, str]:
    """
    Extracts id_repository and run_name from a column inside a dataframe row S3 path based on its bucket structure.

    - For paths containing "bgsi-data-citus-output":
        * run_name: the folder name immediately after the bucket.
        * id_repository: the portion of run_name before the underscore.

    - For paths containing "bgsi-data-illumina":
        * run_name: the substring (before the first dash) of the folder immediately following 'pro/analysis/'.
        * id_repository: the portion of run_name before the underscore.

    Returns a tuple (id_repository, run_name) or (None, None) if no pattern is matched.
    """
    s3_path = pd_row[col]
    if "bgsi-data-citus-output" in s3_path:
        pattern = r"bgsi-data-citus-output/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            run_name = match.group(1)
            id_repository = run_name.split("_")[0]
            return id_repository, run_name
    # s3://bgsi-data-illumina/pro/analysis/0H0124401C01_5e62498f_rerun_2024-08-13_012704-DRAGEN_Germline_WGS_4-2-7_sw-mode-JK-eb5da97e-ea84-416d-8e3e-eeb7733f49fb/0H0124401C01/0H0124401C01.cram
    elif "bgsi-data-illumina" in s3_path:
        pattern = r"bgsi-data-illumina/pro/analysis/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            long_run_name = match.group(1)
            run_name = long_run_name.split("-")[0]
            id_repository = run_name.split("_")[0]
            return id_repository, run_name
    # Example s3://bgsi-data-citus-output-liquid/0E0050901C01_9fb9a9ea/0E0050901C01.cram/0E0050901C01.cram
    elif "bgsi-data-citus-output-liquid" in s3_path:
        pattern = r"bgsi-data-citus-output-liquid/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            long_run_name = match.group(1)
            run_name = long_run_name.split("-")[0]
            id_repository = run_name.split("_")[0]
            return id_repository, run_name
    return "", ""


def _get_report_info(pd_row: pd.DataFrame, col:str, client:BaseClient, bucket_name:str, key:str, report_type=["ind", "eng"]) -> Tuple[str, str, str, str]:
    """
    Constructs the S3 URL for a report based on sample_id and report_type ("ind" or 'eng').
    """
    res = []
    sample_id = pd_row[col]
    for lang in report_type:
        temp = key
        file_key = temp.format(sample_id=sample_id, report_type=lang)
        try:
            response = client.head_object(Bucket=bucket_name, Key=file_key)
            date_modified = response['LastModified']
            s3_path = f"s3://{bucket_name}/{file_key}"
            res.extend([s3_path, date_modified])
        except ClientError as e:
            res.extend(["", ""])

    # Get pipeline information from one of the json files, since the version is the same for both languages.
    try:
        response = client.get_object(Bucket=bucket_name, Key=key.format(sample_id=sample_id, report_type=report_type[0]))
        data = response['Body'].read().decode('utf-8')
        data = json.loads(data)
        version = data.get("version", "")
        res.extend([version])
    except ClientError as e:
        res.extend([""])

    # Ensure the result is always a tuple of four elements
    while len(res) < 5:
        res.append("")
    return tuple(res[:5])


def get_pgx_report_and_dump(input_bucket_name: str, output_bucket_name: str, dwh_bucket_name: str, object_path: str, db_secret_url: str, aws_conn_id=AWS_CONN_ID, **kwargs) -> None:
    """
    Get PGx report from s3 and dump it into a single csv inside bronze bucket
    """

    s3_client = boto3.client("s3",
                             aws_access_key_id=Connection.get_connection_from_secrets(
                                 AWS_CONN_ID).login,
                             aws_secret_access_key=Connection.get_connection_from_secrets(
                                 AWS_CONN_ID).password,
                             region_name="ap-southeast-3")

    # ================ Get .csv files for PGx input ================
    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {'Bucket': input_bucket_name}
    page_iterator = paginator.paginate(**operation_parameters)

    # Get .csv filles for PGx input
    print("Get PGx input files from S3")
    files = []
    for page in page_iterator:
        # Page object returns: ResponseMetadata and the response Contentents.
        # Example: {'Key': 'foo.csv', 'LastModified': datetime.datetime(2025, 2, 5, 14, 0, 36, tzinfo=tzutc()), 'ETag': '"123455"', 'ChecksumAlgorithm': ['CRC64NVME'], 'Size': 608, 'StorageClass': 'STANDARD'}
        files.extend([obj for obj in page.get('Contents', [])
                     if obj["Key"].endswith(".csv")])

    # Read csv for each object
    print("Get input information from sample sheets")
    df = pd.DataFrame()
    for i, file in enumerate(files):
        if i % 100 == 0:
            print(f"Processing file {i} of {len(files)}: {file['Key']}")
        # Get object, parse the body string, and decode into utf-8 (i.e. the '\n' newline), and concat into pandas.
        temp_df = pd.read_csv(StringIO(s3_client.get_object(
            Bucket=input_bucket_name, Key=file["Key"])["Body"].read().decode('utf-8')))
        temp_df["file_name"] = file["Key"].split(".")[0]
        temp_df["input_creation_date"] = file["LastModified"]

        temp_df[["id_repository", "run_name"]] = temp_df.apply(
            extract_info, col="bam", axis=1, result_type="expand")

        df = pd.concat([df, temp_df], ignore_index=True)
    print("Finished get input metadata")

    # ================ Get .csv files for PGx Output ================
    print("Get output metadata for each input which taken from the samlesheets")
    df[["report_path_ind", "ind_report_creation_date", "report_path_eng", "eng_report_creation_date", "pipeline_version"]] = df.apply(
        _get_report_info, col="sample_id", client=s3_client, bucket_name=output_bucket_name, key=OUTPUT_PATH_TEMPLATE, axis=1, result_type="expand")
    s3_client.close()

    df = df.loc[:, ["file_name", "bam", "input_creation_date", "id_repository", "hub_name", "run_name",
                    "report_path_ind", "ind_report_creation_date", "report_path_eng", "eng_report_creation_date", "pipeline_version"]]
    df.astype(str)
    df.fillna(value="", inplace=True)
    print("Finished get output metadata")

    # Fix id_repository since sometimes input valeus are broken
    # Read from DWH
    print("Get id_repository fix from DWH")
    query = """
        SELECT DISTINCT id_repository, new_repository FROM superset_dev.dynamodb_fix_id_repository_latest;
    """
    engine = create_engine(db_secret_url)

    print(f" == Get PGx report list == ")
    with engine.connect() as conn:  # type: ignore
        df_fix_id_repo = pd.read_sql(
            sql=query,
            con=conn.connection
        )

    # To ensure that the join works correctly, we need to convert the id_repository columns to int64 type.
    df['id_repository'] = df['id_repository'].astype(str)
    df_fix_id_repo['id_repository'] = df_fix_id_repo['id_repository'].astype(str)

    print("Fixing id_repository")
    # https://stackoverflow.com/questions/50649853/trying-to-merge-2-dataframes-but-get-valueerror
    df = df.merge(df_fix_id_repo, on="id_repository",
                 how="left", suffixes=('', '_right'))
    
    df["temp_id_repository"] = df.new_repository.combine_first(
        df.id_repository)
    df.drop(columns=["id_repository", "new_repository"], inplace=True)
    df.rename(columns={"temp_id_repository": "id_repository"}, inplace=True)

    df = dict_csv_buf_transform(df.to_dict(orient="records"))

    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"

    print(f"Dumping PGx report to {file_name} in S3 bucket {dwh_bucket_name}")
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=df,
        key=file_name,
        bucket_name=dwh_bucket_name,
        replace=True
    )


def transform_pgx_logs_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    df = df[["file_name", "bam", "input_creation_date", "id_repository", "hub_name", "run_name", "report_path_ind", "ind_report_creation_date", "report_path_eng", "eng_report_creation_date", "pipeline_version"]]
    
    # Convert date_start to datetime for sorting
    df["input_creation_date"] = pd.to_datetime(df["input_creation_date"])
    df["ind_report_creation_date"] = pd.to_datetime(
        df["ind_report_creation_date"])
    df["eng_report_creation_date"] = pd.to_datetime(
        df["eng_report_creation_date"])
    df.sort_values(by=['input_creation_date',
                   'ind_report_creation_date'], ascending=False, inplace=True)

    # Filter only either input_creation_date or ind_report_creation_date is yesterday.

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    print(df.dtypes)
    return df


with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    staging_pgx_report_status_loader_query = f.read()

fetch_and_dump_task = PythonOperator(
    task_id="bronze_pgx",
    python_callable=get_pgx_report_and_dump,
    dag=dag,
    op_kwargs={
        "input_bucket_name": INPUT_BUCKET_NAME,
        "output_bucket_name": OUTPUT_BUCKET_NAME,
        "dwh_bucket_name": S3_DWH_BRONZE,
        "object_path": OBJECT_PATH,
        "aws_conn_id": AWS_CONN_ID,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}",
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
        "object_path": OBJECT_PATH,
        "transform_func": transform_pgx_logs_data,
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}"
    },
    templates_dict={"insert_query": staging_pgx_report_status_loader_query},
    provide_context=True
)

fetch_and_dump_task >> silver_transform_to_db_task  # type: ignore
