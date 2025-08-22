import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from dateutil.parser import isoparse
import requests
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import urllib.parse
import re

# ----------------------------
# Bronze: Fetch from API and Dump to S3
# ----------------------------
logger = LoggingMixin().log

def fetch_bclconvert_and_dump(aws_conn_id, bucket_name, object_path, api_base,
                               transform_func=None, curr_ds=None, **kwargs):
    logger = LoggingMixin().log
    API_BASE=api_base
    curr_ds = kwargs["ds"]
    curr_date_start = datetime.strptime(curr_ds, "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=1)
    curr_date_end = curr_date_start + timedelta(days=2)
    headers = {
        "Authorization": f"Bearer {Variable.get('BSSH_APIKEY1')}",
        "Content-Type": "application/json"
    }
    logger.info(f"📅 Fetching sessions for: {curr_ds}")

    limit = 25
    offset = 0
    all_rows = []

    while True:
        resp = requests.get(
                f"{API_BASE}/appsessions?offset={offset}&limit={limit}"
                f"&sortBy=DateCreated&sortDir=Desc&DateCreated>={curr_ds}",
                headers=headers
        )

        resp.raise_for_status()
        sessions = resp.json().get("Items", [])
        if not sessions:
            break

        for session in sessions:
            name = session.get("Name", "")
            if "BCLConvert" not in name:
                continue

            session_id = session["Id"]
            logger.info(f"🆔 Found BCLConvert session: {session_id} | {name}")
            logger.info(f"🔎 Trying AppSession ID: {session_id} — GET {API_BASE}/appsessions/{session_id}")

            detail_resp = requests.get(f"{API_BASE}/appsessions/{session_id}", headers=headers)
            if detail_resp.status_code != 200:
                logger.warning(f"⚠ Failed to fetch session detail for {session_id}: {detail_resp.status_code}")
                continue

            detail = detail_resp.json()

            properties = {
                item["Name"]: item.get("Content")
                for item in detail.get("Properties", {}).get("Items", [])
                if item.get("Name")
            }

            run_items = []
            for item in detail.get("Properties", {}).get("Items", []):
                if item.get("Name") == "Input.Runs":
                    run_items = item.get("RunItems", [])

            for run in run_items:
                all_rows.append({
                    "RowType": "Run",
                    "SessionId": session_id,
                    "SessionName": detail.get("Name"),
                    "DateCreated": detail.get("DateCreated"),
                    "DateModified": detail.get("DateModified"),
                    "ExecutionStatus": detail.get("ExecutionStatus"),
                    "ICA_Link": detail.get("HrefIcaAnalysis"),
                    "ICA_ProjectId": properties.get("ICA.ProjectId"),
                    "WorkflowReference": properties.get("ICA.WorkflowSessionUserReference"),
                    "RunId": run.get("Id"),
                    "RunName": run.get("Name"),
                    "PercentGtQ30": run.get("SequencingStats", {}).get("PercentGtQ30"),
                    "FlowcellBarcode": run.get("FlowcellBarcode"),
                    "ReagentBarcode": run.get("ReagentBarcode"),
                    "Status": run.get("Status"),
                    "ExperimentName": run.get("ExperimentName"),
                    "RunDateCreated": run.get("DateCreated")
                })

            logs_tail = next(
                (item.get("Content") for item in detail.get("Properties", {}).get("Items", [])
                 if item.get("Name") == "Logs.Tail"),
                ""
            )

            for line in logs_tail.splitlines():
                if "Computed yield for biosample" in line:
                    match = re.search(
                        r"Computed yield for biosample '([^']+)' \(Id: (\d+)\): (\d+) Bps", line)
                    if match:
                        biosample_name = match.group(1)
                        biosample_id = match.group(2)
                        yield_bps = match.group(3)

                        gen_sample_match = re.search(
                            rf"{biosample_name}.*?Generated new Sample: (\d+)",
                            logs_tail, re.DOTALL)
                        generated_sample_id = gen_sample_match.group(1) if gen_sample_match else None

                        all_rows.append({
                            "RowType": "BioSample",
                            "SessionId": session_id,
                            "SessionName": detail.get("Name"),
                            "DateCreated": detail.get("DateCreated"),
                            "RunName": run.get("Name"),
                            "ExperimentName": run.get("ExperimentName"),
                            "RunDateCreated": run.get("DateCreated"),
                            "BioSampleName": biosample_name,
                            "BioSampleId": biosample_id,
                            "ComputedYieldBps": yield_bps,
                            "GeneratedSampleId": generated_sample_id
                        })

        offset += limit

    logger.info(f"✔ Total rows parsed: {len(all_rows)}")

    df = pd.DataFrame(all_rows)
    df = transform_func(df, curr_ds) if transform_func else df
    
    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)

    logger.info(f"✔ Final DataFrame shape: {df.shape}")

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3_path = f"{object_path}/bclconvert_appsessions-{curr_ds}.csv"
    s3.load_string(buffer.getvalue(), s3_path, bucket_name=bucket_name, replace=True)

    logger.info(f"✅ Saved to S3: s3://{bucket_name}/{s3_path}")
    print(f"✅ Saved to S3: {s3_path}")


def create_download_url(api_key: str, project_id: str, file_id: str, BASE_URL:str) -> str:
    url = f"{BASE_URL}/projects/{project_id}/data/{file_id}:createDownloadUrl"
    headers = {
        "accept": "application/vnd.illumina.v3+json",
        "X-API-Key": api_key
    }
    response = requests.post(url, headers=headers, data='')
    response.raise_for_status()
    return response.json().get("url")

def fetch_bclconvertDemux_and_dump(aws_conn_id, bucket_name, object_path, API_KEY, BASE_URL, PROJECT_ID,
                                   transform_func=None, curr_ds=None, **kwargs):
    analyses = []
    page_size = 100
    page_offset = 0
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    logger = LoggingMixin().log
    curr_ds = kwargs["ds"]
    curr_date_start = datetime.strptime(curr_ds, "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=1)
    curr_date_end = curr_date_start + timedelta(days=2)

    HEADERS = {
        "accept": "application/vnd.illumina.v3+json",
        "X-API-Key": API_KEY
    }

    logger.info(f"Fetching sessions for: {curr_ds}")

    while True:
        url = (
            f"{BASE_URL}/projects/{PROJECT_ID}/analyses"
            f"?pageSize={page_size}&pageOffset={page_offset}&sort=reference%20desc"
        )
        logger.info(f"Requesting URL: {url}")
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        analyses.extend(items)

        logger.info(f"Fetched {len(items)} analyses (offset {page_offset})")

        if len(items) < page_size:
            break
        page_offset += page_size

    if not analyses:
        logger.info("No analyses found.")
        return

    # Sort by timeCreated (latest first)
    latest_analyses = sorted(analyses, key=lambda a: a["timeCreated"], reverse=True)

    def extract_lp_reference(reference_str):
        match = re.search(r"(LP[-_]?\d{7}(?:-P\d)?(?:[-_](?:rerun|redo))?)", reference_str, re.IGNORECASE)
        return match.group(1) if match else None

    for analysis in latest_analyses:
        try:
            reference = analysis.get("reference")
            logger.info(f"Checking analysis reference: {reference}")
            if not reference:
                continue

            lp_ref = extract_lp_reference(reference)
            if not lp_ref:
                logger.warning(f"Could not extract LP reference from: {reference}")
                continue

            # --- Handle Demultiplex_Stats.csv ---
            demux_file_path = f"/ilmn-analyses/{reference}/output/Reports/Demultiplex_Stats.csv"
            demux_encoded = urllib.parse.quote(demux_file_path)

            demux_query = (
                f"{BASE_URL}/projects/{PROJECT_ID}/data"
                f"?filePath={demux_encoded}"
                f"&filenameMatchMode=EXACT"
                f"&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
                f"&status=AVAILABLE&type=FILE"
            )

            demux_response = requests.get(demux_query, headers=HEADERS)
            demux_response.raise_for_status()
            demux_items = demux_response.json().get("items", [])

            if demux_items:
                file_id = demux_items[0]["data"]["id"]
                download_url = create_download_url(API_KEY, PROJECT_ID, file_id, BASE_URL)
                response = requests.get(download_url)
                response.raise_for_status()

                s3_key = f"{object_path}/{reference}/{lp_ref}_Demultiplex_Stats.csv"
                s3.load_bytes(
                    bytes_data=response.content,
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=True
                )
                logger.info(f"Uploaded Demultiplex_Stats to: s3://{bucket_name}/{s3_key}")
            else:
                logger.info(f"Demultiplex_Stats.csv not found for {reference}")

            # Quality_Metrics.csv
            quality_file_path = f"/ilmn-analyses/{reference}/output/Reports/Quality_Metrics.csv"
            quality_encoded = urllib.parse.quote(quality_file_path)
            
            quality_query = (
                f"{BASE_URL}/projects/{PROJECT_ID}/data"
                f"?filePath={quality_encoded}"
                f"&filenameMatchMode=EXACT"
                f"&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
                f"&status=AVAILABLE&type=FILE"
            )
            
            quality_response = requests.get(quality_query, headers=HEADERS)
            quality_response.raise_for_status()
            quality_items = quality_response.json().get("items", [])
            
            if quality_items:
                file_id = quality_items[0]["data"]["id"]
                download_url = create_download_url(API_KEY, PROJECT_ID, file_id)
                response = requests.get(download_url)
                response.raise_for_status()
            
                # Final S3 key format: illumina/qs/{file_id}/Quality_Metrics.csv
                qs_s3_key = f"illumina/qs/{file_id}/Quality_Metrics.csv"
                s3.load_bytes(
                    bytes_data=response.content,
                    key=qs_s3_key,
                    bucket_name="bgsi-data-dwh-bronze",
                    replace=True
                )
                logger.info(f"Uploaded Quality_Metrics to: s3://bgsi-data-dwh-bronze/{qs_s3_key}")
            else:
                logger.info(f"Quality_Metrics.csv not found for {reference}")

        except Exception as e:
            logger.error(f"Error processing analysis {reference}: {e}", exc_info=True)
            continue


# def fetch_yield_and_dump(aws_conn_id, bucket_name, object_path, transform_func=None, **kwargs):
#     def get_boto3_client_from_connection(conn_id='aws_default', service='s3'):
#         conn = Connection.get_connection_from_secrets(conn_id)
#         return boto3.client(
#             service,
#             aws_access_key_id=conn.login,
#             aws_secret_access_key=conn.password
#         )
#     def clean_biosample_column(df, column="BioSampleName"):
#         """Standardize biosample column for consistent merging."""
#         df[column] = (
#             df[column]
#             .astype(str)
#             .str.strip()
#             .str.upper()
#             .str.replace(r"\s+", "", regex=True)
#         )
#         return df
#     curr_ds = kwargs['ds']

#     #  Load latest BCL AppSession
#     s3 = get_boto3_client_from_connection(conn_id=aws_conn_id)
#     appsession_prefix = "bssh/appsessions/"
#     paginator = s3.get_paginator("list_objects_v2")
#     page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=appsession_prefix)

#     latest_obj = None
#     for page in page_iterator:
#         for obj in page.get("Contents", []):
#             if obj["Key"].endswith(".csv") and "bclconvert_appsessions" in obj["Key"]:
#                 if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
#                     latest_obj = obj

#     if not latest_obj:
#         logger.warning(" No BCLConvert AppSession file found.")
#         return

#     bcl_key = latest_obj["Key"]
#     logger.info(f" Using BCLConvert AppSession file: {bcl_key}")
#     obj = s3.get_object(Bucket=bucket_name, Key=bcl_key)
#     bcl_df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

#     bcl_df["RunId"] = bcl_df["RunId"].astype(str).str.strip().str.split(".").str[0]
#     bcl_df = clean_biosample_column(bcl_df, "BioSampleName")

#     # Add demux metrics
#     # Add demux metrics (read from S3; no XCom)
#     logger.info("📦 Loading Demultiplex metrics from S3...")
#     ds = kwargs["ds"]
#     demux_key = f"{PREFIX}demux_metrics_{ds}.csv"
    
#     try:
#         demux_obj = s3.get_object(Bucket=bucket_name, Key=demux_key)
#         demux_df = pd.read_csv(StringIO(demux_obj["Body"].read().decode("utf-8")))
#         demux_df = clean_biosample_column(demux_df, "BioSampleName")
#         bcl_df = pd.merge(bcl_df, demux_df, on="BioSampleName", how="left")
#         logger.info(f"✅ Demultiplex metrics merged from s3://{bucket_name}/{demux_key}")
#     except ClientError as e:
#         if e.response.get("Error", {}).get("Code") == "NoSuchKey":
#             logger.warning(f"⚠️ Demultiplex metrics not found at s3://{bucket_name}/{demux_key}")
#         else:
#             logger.error(f"❌ Failed to load demux metrics from S3: {e}")

#     # Append Yield
#     logger.info("🔗 Appending Yield data...")
#     bcl_df = load_yield_csv(bcl_df)
#     bcl_df.loc[bcl_df["RowType"] != "BioSample", "Yield"] = None

#     # Fetch Total Flowcell Yield from API
#     logger.info("🔗 Fetching Flowcell-level Yield totals...")
#     bcl_df["TotalFlowcellYield"] = None
#     run_rows = bcl_df[bcl_df["RowType"] == "Run"]

#     for _, row in run_rows.iterrows():
#         run_id = row.get("RunId")
#         if not run_id or run_id.lower() == "nan":
#             continue
    
#         api_url = f"{API_BASE_URL}/{run_id}/sequencingstats"
#         headers = {
#             "x-access-token": API_TOKEN,
#             "Accept": "application/json"
#         }
    
#         try:
#             logger.info(f"📡 Requesting TotalFlowcellYield for RunId={run_id}")
#             response = requests.get(api_url, headers=headers)
#             response.raise_for_status()
#             data = response.json()
#             total_yield = data.get("YieldTotal")
    
#             if total_yield is not None:
#                 bcl_df.loc[
#                     (bcl_df["RowType"] == "Run") & (bcl_df["RunId"] == run_id),
#                     "TotalFlowcellYield"
#                 ] = total_yield
#                 logger.info(f"✅ Assigned TotalFlowcellYield={total_yield} to RunId={run_id}")
#             else:
#                 logger.warning(f"⚠️ No YieldTotal found for RunId={run_id}")
    
#         except Exception as e:
#             logger.error(f"❌ API error for RunId={run_id}: {e}")
    
#     # After all runs have been processed, extract latest 200
#     try:
#         logger.info("📦 Filtering for the latest 200 Runs and BioSamples...")
        
#         # Ensure DateCreated is datetime
#         bcl_df["DateCreated"] = pd.to_datetime(bcl_df["DateCreated"], errors="coerce")

#         # Show all rows/columns in logs
#         pd.set_option("display.max_rows", None)
#         pd.set_option("display.max_columns", None)
#         pd.set_option("display.width", 0)
#         pd.set_option("display.max_colwidth", None)

#         chunk_size = 24

#         # ✅ Log latest 200 Run rows
#         latest_runs = (
#             bcl_df[bcl_df["RowType"] == "Run"]
#             .sort_values("DateCreated", ascending=False)
#             .head(200)
#         )
#         logger.info("📋 Final latest 200 Run rows (full preview):")
#         for i in range(0, len(latest_runs), chunk_size):
#             chunk = latest_runs.iloc[i:i+chunk_size]
#             logger.info(f"\n🧾 Runs {i+1}–{i+len(chunk)}:\n{chunk.to_string(index=False)}")

#         # ✅ Log latest 200 BioSample rows
#         latest_samples = (
#             bcl_df[bcl_df["RowType"] == "BioSample"]
#             .sort_values("DateCreated", ascending=False)
#             .head(200)
#         )
#         logger.info("📋 Final latest 200 BioSample rows (full preview):")
#         for i in range(0, len(latest_samples), chunk_size):
#             chunk = latest_samples.iloc[i:i+chunk_size]
#             logger.info(f"\n🔬 BioSamples {i+1}–{i+len(chunk)}:\n{chunk.to_string(index=False)}")

#     except Exception as e:
#         logger.warning(f"⚠️ Failed to extract or print latest Run and BioSample rows: {e}")
#     # ✅ Write final output CSV to S3 (bronze/final_output)
#     try:
#         out_df = transform_func(bcl_df.copy(), curr_ds) if transform_func else bcl_df
#         out_key = f"{object_path}/{curr_ds}.csv"
#         s3.put_object(Bucket=bucket_name, Key=out_key, Body=out_df.to_csv(index=False).encode("utf-8"))
#         logger.info(f"✅ Wrote final output to s3://{bucket_name}/{out_key}")
#     except Exception as e:
#         logger.error(f"❌ Failed to write final output to S3: {e}")
#         raise