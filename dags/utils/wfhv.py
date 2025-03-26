
from typing import Tuple, List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re
from io import StringIO
from datetime import timezone
import json
import pandas as pd


def fetch_wfhv_samples_dump_data(aws_conn_id: str, wfhv_input_bucket: str, bronze_bucket: str, bronze_object_path: str, **kwargs):
    """
    Fetches WFHV samples data from the S3 input bucket.

    Parameters:
    ----------
    aws_conn_id : str
        AWS connection ID for authentication.
    wfhv_input_bucket : str
        S3 bucket containing WFHV output data.
    ts : str
        Timestamp used to filter folders based on modification date.
    bronze_bucket : str
        The name of the dwh bronze bucket
    bronze_object_path : str
        The path to store the bronze object
    kwargs : dict
        Additional parameters, including Airflow's execution context (e.g., `ds`)
    """
    # Initialize AWS Hook
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_client = s3_hook.get_conn()
    
    # Use Airflow Execution Date (ds) or a default timestamp
    # ts = str(kwargs.get("ds", "2025-03-02"))
    ts= "2024-03-02"
    # Function to list relevant run folders
    def _get_s3_file(prefix: str, pattern: str):
        """Finds the first file matching `pattern` under `prefix` in S3."""
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=wfhv_input_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if re.search(pattern, obj["Key"]):
                    print(f"Found matching file: {obj['Key']}")
                    return obj["Key"]
        print(f"No matching file found for pattern {pattern} under {prefix}")
        return None

    def _read_s3_file(key: str):
        """Reads an S3 file and returns its content as a string."""
        if key:
            try:
                response = s3_client.get_object(Bucket=wfhv_input_bucket, Key=key)
                return response["Body"].read().decode("utf-8", errors="ignore")
            except Exception as e:
                print(f"Error reading {key}: {e}")
        return None

    def _extract_json_from_html(html_content, key):
        """Extracts a JSON array from HTML content using regex."""
        match = re.search(fr'"{key}"\s*:\s*(\[\{{.*?\}}\])', html_content, re.DOTALL)
        return json.loads(match.group(1)) if match else None

    def _extract_summary_data(prefix: str):
        """Extracts timestamps and metadata from `final_summary*.txt`."""
        content = _read_s3_file(_get_s3_file(prefix, r"final_summary.*\.txt$"))
        if not content:
            print(f"Warning: No summary data found for {prefix}")
            return None, None, None, None, None, None

        def _safe_search(pattern, text):
            match = re.search(pattern, text)
            return match.group(1) if match else None

        return (
            _safe_search(r"started=(\S+)", content),
            _safe_search(r"acquisition_stopped=(\S+)", content),
            _safe_search(r"processing_stopped=(\S+)", content),
            _safe_search(r"instrument=(\S+)", content),
            _safe_search(r"position=(\S+)", content),
            _safe_search(r"flow_cell_id=(\S+)", content),
        )

    def _extract_folder_size_and_date(bucket_name, prefix):
        """
        Calculate the total size of all objects in a folder and get the most recent upload date.
        """
        total_size = 0
        latest_date = None
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    total_size += obj["Size"]
                    obj_date = obj["LastModified"]
                    if latest_date is None or obj_date > latest_date:
                        latest_date = obj_date

        formatted_date = latest_date.strftime('%Y-%m-%d %H:%M:%S') if latest_date else None
        return total_size, formatted_date

    def _extract_barcode_data(prefix: str):
        """Extracts barcode data from an HTML report file."""
        html = _read_s3_file(_get_s3_file(prefix, r"report.*\.html$"))
        barcodes = _extract_json_from_html(html, "barcode_reads") if html else []

        started, acquisition_stopped, processing_stopped, instrument, position, flow_cell_id = _extract_summary_data(prefix)
        
        folder_size, latest_upload_date = _extract_folder_size_and_date(wfhv_input_bucket, prefix)
        
        data = [
            {
                "id_library": prefix.split("/")[0],
                "bam_folder": f"s3://{wfhv_input_bucket}/{prefix}bam_pass/{b['barcode']}/",
                "alias": b["barcode"],
                "id_repository": str(b["barcode"].split("_")[0]) if isinstance(b["barcode"], str) else "UNKNOWN",
                "total_bases": b["total_bases"],
                "passed_bases_percent": b["passed_bases_percent"],
                "total_passed_bases": int(b["total_bases"] * (b["passed_bases_percent"] / 100)),
                "bam_size": folder_size,
                "date_upload": latest_upload_date,
                "started_at": started,
                "acquisition_stopped": acquisition_stopped,
                "processing_stopped": processing_stopped,
                "instrument": instrument,
                "position": position,
                "id_flowcell": flow_cell_id,
            }
            for b in barcodes
        ]

        return pd.DataFrame(data)  # Ensure return is a DataFrame
    def _list_run_folders():
        paginator = s3_client.get_paginator("list_objects_v2")
        recent_runs = set()

        for page in paginator.paginate(Bucket=wfhv_input_bucket, Delimiter="/"):
            for folder in page.get("CommonPrefixes", []):
                prefix = folder["Prefix"].strip("/")
                head_response = s3_client.list_objects_v2(Bucket=wfhv_input_bucket, Prefix=prefix, MaxKeys=1)
                if "Contents" in head_response:
                    last_modified = head_response["Contents"][0]["LastModified"].strftime("%Y-%m-%d")
                    if last_modified >= ts:
                        recent_runs.add(prefix)

        return list(recent_runs)

    # Extract barcode data and process folders (unchanged functions)
    def _process_folders(runname: str):
        """Processes subfolders and extracts barcode data."""
        prefix = f"{runname}/no_sample/"
        matching_data = []
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=wfhv_input_bucket, Prefix=prefix, Delimiter="/"):
            for folder in page.get("CommonPrefixes", []):
                barcode_data = _extract_barcode_data(folder["Prefix"])
                if barcode_data.empty:
                    print(f"No barcode data found for {folder['Prefix']}, skipping...")
                    continue

                matching_data.append(barcode_data)

        return pd.concat(matching_data, ignore_index=True) if matching_data else pd.DataFrame()

    # Main execution
    run_folders = _list_run_folders()
    print(f"Listing runs after {ts}")
    print(run_folders)
    all_data = pd.DataFrame()

    for run in run_folders:
        try:
            data = _process_folders(run)
            if not data.empty:
                all_data = pd.concat([all_data, data], ignore_index=True)
            else:
                print(f"No matching data found for {run}.")
        except Exception as e:
            print(f"Error processing {run}: {e}")

    # Save DataFrame to CSV in S3
    csv_buffer = StringIO()
    all_data.to_csv(csv_buffer, index=False)
    
    curr_ds = kwargs.get("curr_ds", "default_date")
    file_name = f"{bronze_object_path}/{kwargs['curr_ds']}.csv"

    # Upload to S3
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_name,
        bucket_name=bronze_bucket,
        replace=True
    )

    return all_data  # Return DataFrame for debugging/logging if needed


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
    # print(df)
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
