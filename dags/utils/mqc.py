from typing import Any, Dict
from sqlalchemy import create_engine
import pandas as pd
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import re
import os

import os
import re
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def fetch_qc_files(aws_conn_id, **kwargs):

    """
    Sync QC files from source S3 buckets to destination buckets based on filename patterns
    and last modified time. Supports dry run mode for previewing actions.

    :param aws_conn_id: Airflow connection ID for AWS credentials
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_client = s3_hook.get_conn()

    DESTINATION_MAP = {
        'bgsi-data-illumina': 'bgsi-data-dragen-qc',
        'bgsi-data-citus-output': 'bgsi-data-citus-qc',
    }

    TEMPLATES = {
        'bgsi-data-illumina': [
            r'.+\.cnv_metrics\.csv$', r'.+\.gvcf_hethom_ratio_metrics\.csv$',
            r'.+\.gvcf_metrics\.csv$', r'.+\.hla_metrics\.csv$',
            r'.+\.mapping_metrics\.csv$', r'.+\.ploidy_estimation_metrics\.csv$',
            r'.+\.qc-coverage-region-1_coverage_metrics\.csv$',
            r'.+\.roh_metrics\.csv$', r'.+\.sv_metrics\.csv$',
            r'.+\.time_metrics\.csv$', r'.+\.vc_hethom_ratio_metrics\.csv$',
            r'.+\.vc_metrics\.csv$', r'.+\.wgs_coverage_metrics\.csv$',
        ],
        'bgsi-data-citus-output': [
            r'.+\.bcftools_stats_metrics\.txt$', r'.+\.het_check_metrics\.csv$',
            r'.+\.mosdepth\.global\.dist_metrics\.txt$',
            r'.+\.mosdepth\.region\.dist_metrics\.txt$',
            r'.+\.mosdepth\.summary_metrics\.txt$', r'.+\.sex_check_metrics\.csv$',
            r'.+_1_fastqc_metrics\.zip$', r'.+_2_fastqc_metrics\.zip$',
            r'.+_metrics\.stats$',
        ]
    }

    # Use Airflow Execution Date (ds) or a default timestamp
    ts = str(kwargs.get("ds", "2025-03-02"))
    # ts="2025-04-02"
    timestamps = datetime.strptime(ts, "%Y-%m-%d").date()
    
    def sync_s3_buckets():
        for src_bucket, dest_bucket in DESTINATION_MAP.items():
            allowed_patterns = TEMPLATES[src_bucket]
            paginator = s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=src_bucket):
                for obj in page.get('Contents', []):
                    src_key = obj['Key']
                    filename = os.path.basename(src_key)

                    # Skip if not modified by timestamps or later
                    last_modified = obj['LastModified'].date()
                    if last_modified < timestamps:
                        continue

                    if not any(re.fullmatch(p, filename) for p in allowed_patterns):
                        continue

                    try:
                        parts = src_key.split('/')
                        if src_bucket == 'bgsi-data-illumina':
                            folder_id, sample_id = parts[2], parts[3]
                            dest_key = f"{folder_id}/{sample_id}/{filename}"
                        elif src_bucket == 'bgsi-data-citus-output':
                            folder_id = parts[0]
                            dest_key = f"{folder_id}/{filename}"
                        else:
                            continue
                    except IndexError:
                        logging.warning(f"Bad key format: {src_key}")
                        continue

                    try:
                        s3_client.head_object(Bucket=dest_bucket, Key=dest_key)
                        logging.info(f"Skipping {src_key}, already exists.")
                        continue
                    except s3_client.exceptions.ClientError as e:
                        if e.response['Error']['Code'] != "404":
                            raise
                    # logging.info(f"[Dry Run] Would copy {src_key} -> {dest_key}")
                    logging.info(f"Copying {src_key} -> {dest_key}")
                    s3_client.copy_object(
                        Bucket=dest_bucket,
                        CopySource={'Bucket': src_bucket, 'Key': src_key},
                        Key=dest_key
                    )

    sync_s3_buckets()


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


    # Only upload if there is data
    if not dragen_result_df.empty:
        upload_df(dragen_paths, "dragen")
    else:
        print("[INFO] No DRAGEN results found. Skipping upload.")

    if not citus_result_df.empty:
        upload_df(citus_paths, "citus")
    else:
        print("[INFO] No CITUS results found. Skipping upload.")
