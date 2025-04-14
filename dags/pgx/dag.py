from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable, Connection
from utils.utils import dict_csv_buf_transform, silver_transform_to_db
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import boto3
import botocore
import pandas as pd
from typing import Tuple
from io import StringIO
import re
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from sqlalchemy import create_engine, VARCHAR, SMALLINT
from concurrent.futures import ThreadPoolExecutor, as_completed

AWS_CONN_ID = "aws"
OBJECT_PATH = "pgx/report_logs"
INPUT_BUCKET_NAME = "nl-data-pgx-input"
OUTPUT_BUCKET_NAME = "nl-data-pgx-output"
OUTPUT_PATH_TEMPLATE = "production/{sample_id}/report/production.{sample_id}.{report_type}.data.pdf"
S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
SIMBIOX_APIKEY = Variable.get("SIMBIOX_APIKEY")
RDS_SECRET = Variable.get("RDS_SECRET")
LOADER_QEURY = "pgx_loader.sql"

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
    elif "bgsi-data-illumina" in s3_path:
        pattern = r"bgsi-data-illumina/pro/analysis/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            long_run_name = match.group(1)
            run_name = long_run_name.split("-")[0]
            id_repository = run_name.split("_")[0]
            return id_repository, run_name
    return None, None


def get_report_info(pd_row: pd.DataFrame, col, client: boto3.s3, bucket_name, key, report_type=["ind", "eng"]) -> Tuple[str, str, str, str]:
    """
    Constructs the S3 URL for a report based on sample_id and report_type ("ind" or 'eng').

    The expected S3 key format is:
      production/{sample_id}/report/production.{sample_id}.{report_type}.data.pdf
    """
    res = []
    sample_id = pd_row[col]
    for lang in report_type:
        key = key.format(sample_id=sample_id, report_type=lang)
        try:
            response = client.head_object(Bucket=bucket_name, Key=key)
            date_modified = response['LastModified']
            s3_path = f"s3://{bucket_name}/{key}"
            res.extend([s3_path, date_modified])
        except botocore.exceptions.ClientError as e:
            # If the file is not found, return (None, None)
            res.extend([None, None])
    return res


def get_pgx_report_and_dump(input_bucket_name: str, output_bucket_name: str, dwh_bucket_name: str, object_path: str, aws_conn_id=AWS_CONN_ID,  **kwargs) -> None:
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
    files = []
    for page in page_iterator:
        # Page object returns: ResponseMetadata and the response Contentents.
        # Example: {'Key': 'foo.csv', 'LastModified': datetime.datetime(2025, 2, 5, 14, 0, 36, tzinfo=tzutc()), 'ETag': '"123455"', 'ChecksumAlgorithm': ['CRC64NVME'], 'Size': 608, 'StorageClass': 'STANDARD'}
        files.extend([obj for obj in page.get('Contents', [])
                     if obj["Key"].endswith(".csv")])

    # Read csv for each object
    df = pd.DataFrame()
    for file in files:
        # Get object, parse the body string, and decode into utf-8 (i.e. the '\n' newline), and concat into pandas.
        temp_df = pd.read_csv(StringIO(s3_client.get_object(
            Bucket=input_bucket_name, Key=file["Key"])["Body"].read().decode('utf-8')))
        temp_df["file_name"] = file["Key"].split(".")[0]
        temp_df["input_creation_date"] = file["LastModified"]

        temp_df[["id_repository", "run_name"]] = temp_df.apply(
            extract_info, col="bam", axis=1, result_type="expand")

        df = pd.concat([df, temp_df], ignore_index=True)

    # ================ Get .csv files for PGx Output ================
    df[["report_path_ind", "ind_report_creation_date", "report_path_eng", "eng_report_creation_date"]] = df.apply(
        get_report_info, col="sample_id", client=s3_client, bucket_name=output_bucket_name, key=OUTPUT_PATH_TEMPLATE, axis=1, result_type="expand")
    s3_client.close()

    df = df.loc[:, ["file_name", "bam", "input_creation_date", "id_repository", "hub_name", "run_name",
                    "report_path_ind", "ind_report_creation_date", "report_path_eng", "eng_report_creation_date"]]
    df.astype(str)
    df.fillna(value="", inplace=True)

    df = dict_csv_buf_transform(df)

    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"

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
    return df


def _get_pgx_report_json_files_from_s3(s3_client:boto3.client, bucket_name:str, key:str) -> pd.DataFrame:
    resp = s3_client.get_object(Bucket=bucket_name, Key=key)["Body"].read().decode('utf-8')
    temp_report = json.loads(resp)

    order_id = temp_report.get("order_details").get("order_id")
    hubs = temp_report.get("order_details").get("ordered_by")
    test_method = temp_report.get("sample_details").get("test_method")

    col = ["drug_name", "phenotype_text", "gene_symbol", "branded_drug", "drug_category", "drug_classification", "nala_score_v2", "rec_category", "rec_source", "rec_text", "run_id", "scientific_evidence_symbol", "implication_text"]

    temp_res = []

    for rec in temp_report.get("clinical_recommendations"):
        temp_res.append(
                (
                rec["drug_name"], rec["phenotype_text"], 
                rec["gene_symbol"], rec["branded_drug"], 
                rec["drug_category"], rec["drug_classification"], 
                rec["nala_score_v2"], rec["rec_category"], rec["rec_source"], rec["rec_text"],
                rec["run_id"], rec["scientific_evidence_symbol"], rec["implication_text"]
                )
             )
    temp_df = pd.DataFrame(temp_res, columns=col)
    temp_df["order_id"] = order_id
    temp_df["hubs"] = hubs
    temp_df["test_method"] = test_method
    return temp_df

def get_pgx_summary(db_secret_url:str) -> None:
    """
    Get PGx report from s3 and dump it into a single csv inside bronze bucket

    Parameters
    ---------
    db_secret_url: str
        The database secret url to connect to the database.
    """
    # Get desired report
    # Get all available order_id from RegINA 
    pgx_report_bucket = "nl-data-pgx-output"
    query="""
    SELECT 
    DISTINCT REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(report_path_ind, "s3://{pgx_report_bucket}/", ""), "pdf", "json"), "report", "report_ind") path
    FROM (SELECT id_subject, report_path_ind, pgx_input_creation_date, ROW_NUMBER() OVER(PARTITION BY id_subject ORDER BY ind_report_creation_date) rn FROM gold_pgx_report WHERE ind_report_creation_date > "{date_filter}" AND report_path_ind IS NOT NULL
    ) t WHERE t.rn = 1 
    """
    query = query.format(pgx_report_bucket=pgx_report_bucket, date_filter="2024-10-01")
    engine=create_engine(db_secret_url)

    print(f" == Get PGx report list == ")
    with engine.connect() as conn:
        pgx_reports = pd.read_sql(
            sql=query,
            con=conn.connection
        )
    
    pgx_reports = pgx_reports["path"].values.tolist()
    s3_client = boto3.client("s3",
                             aws_access_key_id=Connection.get_connection_from_secrets(
                                 AWS_CONN_ID).login,
                             aws_secret_access_key=Connection.get_connection_from_secrets(
                                 AWS_CONN_ID).password,
                             region_name="ap-southeast-3")

    # # Use ThreadPoolExecutor to process all subjects in parallel (adjust max_workers as needed)
    res = []

    print(f" == Get PGx report info from s3 == ")
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(_get_pgx_report_json_files_from_s3, s3_client, pgx_report_bucket, report) for report in pgx_reports]
        for future in as_completed(futures):
            res.append(future.result())
    res = pd.concat(res, ignore_index=True)
    # res.to_csv("pgx_report.csv", index=False)
    res.reset_index(drop=True, inplace=True)
    res.set_index(["order_id", "hubs", "drug_category", "drug_name", "drug_classification"], inplace=True)

    print(f" == Save result to dwh == ")
    with engine.begin() as conn:
        res.to_sql(
                name=f"gold_pgx_summary_report",
                if_exists="replace",
                schema="dwh_restricted",
                con=conn,
                dtype={'hubs': VARCHAR(128), 'drug_name': VARCHAR(128), 'drug_category': VARCHAR(128), 'drug_classification': VARCHAR(128), 'gene_symbol': VARCHAR(16), 'order_id': VARCHAR(36)}
            )
    res.reset_index(inplace=True)
    grouped_res = res.groupby(["hubs", "drug_name", "gene_symbol", "rec_source", "scientific_evidence_symbol", "nala_score_v2", "rec_category"]).size().reset_index(name="ct_subj_id")

    total_id_per_hubs = res.groupby(["hubs"]).agg({"order_id": "nunique"}).reset_index()
    total_id_per_hubs.rename(columns={"order_id": "ct_total_subj_id"}, inplace=True)
    grouped_res = pd.merge(grouped_res, total_id_per_hubs, on="hubs", how="left")
    grouped_res.set_index(["hubs", "drug_name", "gene_symbol", "nala_score_v2", "rec_category"], inplace=True)
    with engine.begin() as conn:
        grouped_res.to_sql(
            name=f"gold_pgx_summary_report",
            if_exists="replace",
            con=conn,
            dtype={"hubs": VARCHAR(128), "drug_name": VARCHAR(128), "gene_symbol": VARCHAR(16), "nala_score_v2": VARCHAR(32), "rec_category": SMALLINT}
        )
    print(f" == Finished == ")

    return True

with open(os.path.join("dags/repo/dags/include/loader", LOADER_QEURY)) as f:
    loader_query = f.read()

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
    templates_dict={"insert_query": loader_query},
    provide_context=True
)

get_pgx_summary_task = PythonOperator(
    task_id="get_pgx_summary",
    python_callable=get_pgx_summary,
    dag=dag,
    op_kwargs={
        "db_secret_url": RDS_SECRET,
        "curr_ds": "{{ ds }}"
    },
    provide_context=True
)

fetch_and_dump_task >> silver_transform_to_db_task >> get_pgx_summary_task
