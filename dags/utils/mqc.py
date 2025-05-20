from typing import Any, Dict
from sqlalchemy import create_engine
import pandas as pd
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def extract_incomplete_qc(aws_conn_id: str, bucket_name: str, dragen_bucket:str, object_path: str, 
                        citus_bucket:str, db_uri: str, **kwargs) -> None:
    """
    Extracts incomplete QC data and matches to relevant S3 paths for CITUS and DRAGEN.
    Writes matched paths to S3 in separate CSVs.
    """
    # Set up SQLAlchemy engine
    engine = create_engine(db_uri)

    # Run query
    query = """
        SELECT id_repository, pipeline_name 
        FROM gold_qc
        WHERE run_name IS NOT NULL 
        AND (pipeline_name = 'CITUS' OR pipeline_name LIKE '%%DRAGEN%%')
        AND (coverage IS NULL OR at_least_10x IS NULL OR ploidy_estimation IS NULL);
    """
    df = pd.read_sql(query, con=engine)
    print(f"[INFO] Pulled {len(df)} rows from gold_qc")

    # Separate DRAGEN and CITUS
    dragen_df = df[df['pipeline_name'].str.contains('DRAGEN', case=False, na=False)]
    citus_df = df[df['pipeline_name'].str.contains('CITUS', case=False, na=False)]

    # S3 setup
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    dragen_paths = []
    citus_paths = []

    # Process DRAGEN
    for id_repo in dragen_df['id_repository'].unique():
        keys = s3_hook.list_keys(bucket_name=dragen_bucket, prefix=f"{id_repo}")
        if keys:
            folders = set()
            for key in keys:
                parts = key.split('/')
                if len(parts) >= 2:
                    folder = f"{parts[0]}/"
                    folders.add(folder)
            for folder in folders:
                dragen_paths.append(f"s3://{dragen_bucket}/{folder}")

    # Process CITUS
    for id_repo in citus_df['id_repository'].unique():
        keys = s3_hook.list_keys(bucket_name=citus_bucket, prefix=f"{id_repo}")
        if keys:
            folders = set()
            for key in keys:
                parts = key.split('/')
                if len(parts) >= 2:
                    folder = f"{parts[0]}/"
                    folders.add(folder)
            for folder in folders:
                citus_paths.append(f"s3://{citus_bucket}/{folder}")

    # Create DataFrames
    dragen_result_df = pd.DataFrame(dragen_paths).drop_duplicates()
    citus_result_df = pd.DataFrame(citus_paths).drop_duplicates()

    # Upload to S3
    curr_ds = kwargs.get("curr_ds", "default_date")

    def upload_df(paths: list, prefix: str):
        buffer = StringIO()
        buffer.write('\n'.join(sorted(set(paths))))  # No header, no duplicates
        buffer.seek(0)
        key = f"{object_path}/{prefix}/{curr_ds}.csv"
        s3_hook.load_string(
            string_data=buffer.getvalue(),
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"[INFO] Saved {prefix.upper()} CSV to s3://{bucket_name}/{key}")


    upload_df(dragen_paths, "dragen")
    upload_df(citus_paths, "citus")
