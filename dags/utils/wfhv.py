
from typing import Tuple, List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re
from io import StringIO
from datetime import timezone
import json
import pandas as pd


def fetch_wfhv_analysis_dump_data(aws_conn_id: str, wfhv_output_bucket: str, bronze_bucket: str, bronze_object_path: str,  **kwargs) -> None:
    """
    Fetch wfhv analysis results from wfhv output bucket to the dwh bronze bucket

    Parameters
    ----------
    aws_conn_id : str
        AWS connection ID
    wfhv_output_bucket : str
        The name of the bucket where the wfhv output is stored
    bronze_bucket : str
        The name of the dwh bronze bucket
    bronze_object_path : str
        The path to store the bronze object
    kwargs : dict

    Returns
    -------
    None 
    """
    include_patterns = [
        r".*cram$",
        r".*_snp\.vcf\.gz$",
        r".*wf-human-alignment-report\.html$"
    ]

    def _match_patterns(file_name, patterns) -> List[str]:
        matches = [None] * len(patterns)
        for i, pattern in enumerate(patterns):
            if re.match(pattern, file_name):
                matches[i] = file_name
        return matches

    def _get_html_file_content(s3_hook: S3Hook, bucket: str, key: str) -> str:
        s3_client = s3_hook.get_conn()
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')

    def _extract_pipeline_version_and_type(html_content: str) -> Tuple[str, str]:
        match = re.search(
            r'wf-human-alignment-report.html</code> nextflow workflow \(([\d.]+)\)', html_content)
        version = match.group(1) if match else None
        pipeline_type = "secondary"
        return version, pipeline_type

    # Fetch data from the Nextflow pipeline
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # fetch prefixes
    folders = s3_hook.list_prefixes(
        bucket_name=wfhv_output_bucket, delimiter="/")
    data = []
    for folder in folders:
        row = {
            'id_repository': folder.split('.')[0].split('_')[0],
            'run_name': folder.rstrip('/'),
            'cram': None,
            'cram_size': None,
            'vcf': None,
            'vcf_size': None,
            'pipeline_name': None,
            'pipeline_type': None,
            'date_start': None
        }
        files = s3_hook.get_file_metadata(
            bucket_name=wfhv_output_bucket, prefix=folder)

        date_start = None
        for file in files:
            file_name = file['Key']
            matches = _match_patterns(file_name, include_patterns)
            if matches[0]:
                row['cram'] = f"s3://{wfhv_output_bucket}/{matches[0]}"
                row['cram_size'] = str(int(file['Size']))
            elif matches[1]:
                row['vcf'] = f"s3://{wfhv_output_bucket}/{matches[1]}"
                row['vcf_size'] = str(int(file['Size']))
            elif matches[2]:
                html_content = _get_html_file_content(
                    s3_hook, wfhv_output_bucket, file_name)
                version, pipeline_type = _extract_pipeline_version_and_type(
                    html_content)
                row['pipeline_name'] = f"wf-human-variation {version}"
                row['pipeline_type'] = pipeline_type

            if not date_start or file['LastModified'] < date_start:
                date_start = file['LastModified']

        row['date_start'] = date_start.astimezone(timezone.utc).isoformat()
        data.append(row)

    df = pd.DataFrame(data)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    print(df)
    file_name = f"{bronze_object_path}/{kwargs['curr_ds']}.csv"

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_name,
        bucket_name=bronze_bucket,
        replace=True
    )


def fetch_wfhv_stats_dump_data(aws_conn_id: str, wfhv_output_bucket: str, bronze_bucket: str, bronze_object_path: str,  **kwargs) -> None:
    """
    Fetch wfhv analysis results from wfhv output bucket to the dwh bronze bucket

    Parameters
    ----------
    aws_conn_id : str
        AWS connection ID
    wfhv_output_bucket : str
        The name of the bucket where the wfhv output is stored
    bronze_bucket : str
        The name of the dwh bronze bucket
    bronze_object_path : str
        The path to store the bronze object
    kwargs : dict

    Returns
    -------
    None 
    """
    # Fetch data from the Nextflow pipeline
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    # fetch prefixes
    folders = s3_hook.list_prefixes(
        bucket_name=wfhv_output_bucket, delimiter="/")
    all_data = []
    for folder in folders:
        folder_contents = s3_hook.get_file_metadata(
            bucket_name=wfhv_output_bucket, prefix=folder)
        stats_files = [
            obj['Key'] for obj in folder_contents
            if obj['Key'].endswith('.stats.json')
        ]
        for stats_file in stats_files:
            try:
                print(f"Processing file: {stats_file}")

                # Fetch and parse the .stats.json file
                obj = s3_hook.get_conn().get_object(Bucket=wfhv_output_bucket, Key=stats_file)
                file_content = obj['Body'].read().decode('utf-8')
                data = json.loads(file_content)

                # Flatten specific fields
                if "Yield (reads >=Nbp)" in data:
                    yield_reads = data.pop("Yield (reads >=Nbp)")
                    for key, value in yield_reads.items():
                        data[f"Yield (reads >={key}bp)"] = value

                if "Bases with >=N-fold coverage" in data:
                    bases_coverage = data.pop("Bases with >=N-fold coverage")
                    for key, value in bases_coverage.items():
                        data[f"Bases with >={key}-fold coverage"] = value

                # Add metadata for traceability
                data['run_name'] = folder.rstrip('/')
                data['file_name'] = stats_file.split('/')[-1]

                all_data.append(data)
            except Exception as e:
                print(f"Could not process {stats_file}: {e}")

    # Convert the data into a DataFrame
    df = pd.DataFrame(all_data)

    # Convert all columns to strings
    df = df.astype(str)

    # Extract additional metadata
    df['id_repository'] = df['run_name'].apply(lambda x: x.split('_')[0])

    print(df.head())

    # Convert DataFrame to CSV format
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    file_name = f"{bronze_object_path}/{kwargs['curr_ds']}.csv"

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_name,
        bucket_name=bronze_bucket,
        replace=True
    )
