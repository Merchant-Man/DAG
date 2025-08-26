from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timezone
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import urllib.parse
import re
import requests
import pandas as pd
import logging
import sys
import io
from typing import Dict, Any, List, Optional, Tuple

logger = LoggingMixin().log


# ---------- util ----------
def _parse_iso(ts: str) -> Optional[datetime]:
    """Accepts 'YYYY-MM-DD' or ISO 'YYYY-MM-DDTHH:MM:SSZ' -> aware UTC datetime."""
    if not ts:
        return None
    try:
        if "T" in ts:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return datetime.strptime(ts, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def fetch_bclconvert_runs_and_biosamples(
    aws_conn_id: str,
    api_base: str,
    api_token: str,
    bucket_name: str,
    run_object_path: str,
    biosample_object_path: str,
    *,
    max_rows: int = 1000,
    page_limit: int = 25,
    sort_by: str = "DateCreated",
    sort_dir: str = "Desc",
    logger: Optional[logging.Logger] = None,
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch newest BCLConvert sessions; build:
      - df_runs: run-level info (+ total_flowcell_yield_gbp)
      - df_biosamples: biosample yields parsed from Logs.Tail
    Upload both CSVs to S3 iff DataFrame is non-empty.
    """
    ds="2025-08-10"
    # ds = kwargs.get("ds") or datetime.utcnow().strftime("%Y-%m-%d")
    cutoff = _parse_iso(ds)

    if logger is None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
        logger = logging.getLogger("bclconvert-fetch")

    run_rows: List[dict] = []
    biosample_rows: List[dict] = []
    offset = 0

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    while len(run_rows) < max_rows:
        url = (
            f"{api_base}/appsessions"
            f"?offset={offset}&limit={page_limit}&sortBy={sort_by}&sortDir={sort_dir}"
        )
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        payload = resp.json() or {}
        sessions = payload.get("Items", []) or []
        if not sessions:
            logger.info("No sessions returned; stopping pagination.")
            break

        # Filter by cutoff using DateModified (fallback to DateCreated)
        if cutoff:
            pre = len(sessions)
            sessions = [
                s for s in sessions
                if ((_parse_iso(s.get("DateModified")) or _parse_iso(s.get("DateCreated"))) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff
            ]
            logger.info(f"Cutoff filter {ds}: kept {len(sessions)}/{pre} sessions in this page.")
            if not sessions:
                break

        for session in sessions:
            if "BCLConvert" not in (session.get("Name") or ""):
                continue
            session_id = session.get("Id")
            if not session_id:
                continue

            # --- detail ---
            detail_url = f"{api_base}/appsessions/{session_id}"
            dresp = requests.get(detail_url, headers=headers)
            if dresp.status_code != 200:
                logger.warning(f"⚠️ Detail fetch failed for {session_id}: {dresp.status_code}")
                continue
            detail = dresp.json() or {}

            props_items = (detail.get("Properties") or {}).get("Items", []) or []
            properties = {i.get("Name"): i.get("Content") for i in props_items if i.get("Name")}

            run_items = []
            for i in props_items:
                if i.get("Name") == "Input.Runs":
                    run_items = i.get("RunItems", []) or []

            # --- Run rows ---
            for run in run_items:
                run_rows.append({
                    "session_id": session_id,
                    "session_name": detail.get("Name"),
                    "date_created": detail.get("DateCreated"),
                    "date_modified": detail.get("DateModified"),
                    "execution_status": detail.get("ExecutionStatus"),
                    "ica_link": detail.get("HrefIcaAnalysis"),
                    "ica_project_id": properties.get("ICA.ProjectId"),
                    "workflow_reference": properties.get("ICA.WorkflowSessionUserReference"),
                    "run_id": run.get("Id"),
                    "run_name": run.get("Name"),
                    "percent_gt_q30": (run.get("SequencingStats") or {}).get("PercentGtQ30"),
                    "flowcell_barcode": run.get("FlowcellBarcode"),
                    "reagent_barcode": run.get("ReagentBarcode"),
                    "status": run.get("Status"),
                    "experiment_name": run.get("ExperimentName"),
                    "run_date_created": run.get("DateCreated"),
                })

                # ---- BioSample rows (Logs.Tail parsing) ----
                logs_tail = next((i.get("Content") for i in props_items if i.get("Name") == "Logs.Tail"), "")
                if logs_tail:
                    for line in logs_tail.splitlines():
                        if "Computed yield for biosample" not in line:
                            continue
                        m = re.search(r"Computed yield for biosample '([^']+)' \(Id: (\d+)\): (\d+)\s+Bps", line)
                        if not m:
                            continue
                        biosample_name, biosample_id, yield_bps = m.group(1), m.group(2), m.group(3)

                        gen_m = re.search(
                            rf"{re.escape(biosample_name)}.*?Generated new Sample:\s+(\d+)",
                            logs_tail,
                            flags=re.DOTALL,
                        )
                        generated_sample_id = gen_m.group(1) if gen_m else None

                        biosample_rows.append({
                            "session_id": session_id,
                            "session_name": detail.get("Name"),
                            "date_created": detail.get("DateCreated"),
                            "run_name": run.get("Name"),
                            "experiment_name": run.get("ExperimentName"),
                            "run_date_created": run.get("DateCreated"),
                            "biosample_name": biosample_name,
                            "biosample_id": biosample_id,
                            "computed_yield_bps": int(yield_bps),
                            "generated_sample_id": generated_sample_id,
                        })

                if len(run_rows) >= max_rows:
                    break
            if len(run_rows) >= max_rows:
                break

        offset += page_limit

    # ---- Build DataFrames ----
    df_runs = pd.DataFrame(run_rows)
    df_biosamples = pd.DataFrame(biosample_rows)

    # ---- Enrich Run rows with sequencing stats ----
    if not df_runs.empty:
        for _, row in df_runs.iterrows():
            run_id = row.get("run_id")
            if not run_id or str(run_id).lower() == "nan":
                continue
            api_url = f"{api_base}/runs/{run_id}/sequencingstats"
            try:
                response = requests.get(api_url, headers={"x-access-token": api_token, "Accept": "application/json"})
                response.raise_for_status()
                data = response.json() or {}
                total_yield = data.get("YieldTotal")
                if total_yield is not None:
                    df_runs.loc[df_runs["run_id"] == run_id, "total_flowcell_yield_Gbps"] = total_yield
            except Exception as e:
                logger.warning(f"Failed fetching stats for run {run_id}: {e}")

    # ---- Save to S3 only if non-empty ----
    s3 = S3Hook(aws_conn_id=aws_conn_id)

    if not df_runs.empty:
        runs_key = f"{run_object_path}/{ds}.csv"
        buf = StringIO()
        df_runs.to_csv(buf, index=False)
        s3.load_string(string_data=buf.getvalue(), key=runs_key, bucket_name=bucket_name, replace=True)
        logger.info(f"✔ Uploaded Runs CSV → s3://{bucket_name}/{runs_key} (shape={df_runs.shape})")
    else:
        logger.info("No runs found.")

    if not df_biosamples.empty:
        bios_key = f"{biosample_object_path}/{ds}.csv"
        buf = StringIO()
        df_biosamples.to_csv(buf, index=False)
        s3.load_string(string_data=buf.getvalue(), key=bios_key, bucket_name=bucket_name, replace=True)
        logger.info(f"✔ Uploaded BioSamples CSV → s3://{bucket_name}/{bios_key} (shape={df_biosamples.shape})")
    else:
        logger.info("No biosamples found")

    logger.info(f"Final shapes: Runs={df_runs.shape}, BioSamples={df_biosamples.shape}")
    return df_runs, df_biosamples


def fetch_demux_qs_ica_to_s3(
    *,
    aws_conn_id: str,
    API_KEY: str,
    PROJECT_ID: str,
    BASE_URL: str,           # e.g. "https://ica.illumina.com/ica/rest/api"
    bucket_name: str,        # destination bucket for both uploads
    object_path_demux: str,  # e.g. "illumina/demux"
    object_path_qs: str,     # e.g. "illumina/qs"
    **kwargs
) -> Dict[str, Any]:
    """
    Fetch ICA analyses (timeModified >= ds), pull Demultiplex_Stats.csv and Quality_Metrics.csv,
    append id_library column, and upload to S3 — but skip upload if CSV is missing or empty.
    """
    def create_download_url(api_key: str, project_id: str, file_id: str, base_url: str) -> str:
        url = f"{base_url}/projects/{project_id}/data/{file_id}:createDownloadUrl"
        headers = {"accept": "application/vnd.illumina.v3+json", "X-API-Key": api_key}
        r = requests.post(url, headers=headers, data="")
        r.raise_for_status()
        return r.json().get("url")

    def parse_iso_utc(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    LP_REGEX = re.compile(r"(LP[-_]?\d{7}(?:-P\d)?(?:[-_](?:rerun|redo))?)", re.IGNORECASE)

    def lookup_file_by_path(file_path: str) -> List[dict]:
        encoded = urllib.parse.quote(file_path)
        q = (
            f"{BASE_URL}/projects/{PROJECT_ID}/data"
            f"?filePath={encoded}"
            f"&filenameMatchMode=EXACT"
            f"&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE"
            f"&status=AVAILABLE&type=FILE"
        )
        rr = requests.get(q, headers=HEADERS)
        rr.raise_for_status()
        return rr.json().get("items", [])

    def download_csv_bytes(file_id: str) -> bytes:
        url = create_download_url(API_KEY, PROJECT_ID, file_id, BASE_URL)
        r = requests.get(url)
        r.raise_for_status()
        return r.content

    def add_id_library(csv_bytes: bytes, id_library: str) -> Optional[bytes]:
        """Return CSV bytes with id_library appended; return None if parsed CSV is empty."""
        buf = io.StringIO(csv_bytes.decode("utf-8"))
        df = pd.read_csv(buf)
        if df.empty:
            return None
        df["id_library"] = id_library
        return df.to_csv(index=False).encode("utf-8")

    # ---------- fetch analyses ----------
    HEADERS = {"accept": "application/vnd.illumina.v3+json", "X-API-Key": API_KEY}
    ds="2025-08-10"
    # ds = kwargs.get("ds") or datetime.utcnow().strftime("%Y-%m-%d")
    cutoff = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    s3 = S3Hook(aws_conn_id=aws_conn_id)

    analyses: List[dict] = []
    page_size = 100
    page_offset = 0

    logger.info(f"[ICA] Fetching analyses updated on/after {ds} (UTC start-of-day cutoff)")

    while True:
        url = (
            f"{BASE_URL}/projects/{PROJECT_ID}/analyses"
            f"?pageSize={page_size}&pageOffset={page_offset}&sort=reference%20desc"
        )
        logger.info(f"[ICA] GET {url}")
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])

        logger.info(f"[ICA] Page offset {page_offset}: {len(items)} analyses")

        for a in items:
            tm = a.get("timeModified")
            if tm and parse_iso_utc(tm) >= cutoff:
                analyses.append(a)

        if len(items) < page_size:
            break
        page_offset += page_size

    latest_analyses = sorted(
        analyses, key=lambda a: parse_iso_utc(a["timeModified"]), reverse=True
    ) if analyses else []

    summary: Dict[str, Any] = {
        "analyses_considered": len(latest_analyses),
        "analyses_processed": 0,
        "demux_uploaded": 0,
        "quality_uploaded": 0,
        "per_analysis": []
    }

    for analysis in latest_analyses:
        reference = analysis.get("reference")
        if not reference:
            continue

        status_row = {"reference": reference, "demux": "not_found", "quality": "not_found"}

        m = LP_REGEX.search(str(reference))
        if not m:
            logger.warning(f"[ICA] Could not extract id_library from reference: {reference}")
            summary["per_analysis"].append(status_row)
            continue
        id_library = m.group(1)

        # --- Demultiplex_Stats.csv ---
        try:
            demux_path = f"/ilmn-analyses/{reference}/output/Reports/Demultiplex_Stats.csv"
            demux_items = lookup_file_by_path(demux_path)

            if demux_items:
                demux_file_id = demux_items[0]["data"]["id"]
                raw_bytes = download_csv_bytes(demux_file_id)
                out_bytes = add_id_library(raw_bytes, id_library)

                if out_bytes is not None:
                    demux_s3_key = f"{object_path_demux}/{reference}/{id_library}_Demultiplex_Stats.csv"
                    s3.load_bytes(bytes_data=out_bytes, key=demux_s3_key, bucket_name=bucket_name, replace=True)
                    logger.info(f"[S3] Uploaded Demultiplex_Stats → s3://{bucket_name}/{demux_s3_key}")
                    status_row["demux"] = "uploaded"
                    summary["demux_uploaded"] += 1
                else:
                    logger.info(f"[ICA] Demultiplex_Stats.csv empty for {reference}, skipping upload")
                    status_row["demux"] = "empty_skipped"
            else:
                logger.info(f"[ICA] Demultiplex_Stats.csv not found for {reference}")

        except Exception as e:
            logger.error(f"[ICA] Error processing Demultiplex_Stats for {reference}: {e}", exc_info=True)
            status_row["demux"] = "error"

        # --- Quality_Metrics.csv ---
        try:
            quality_path = f"/ilmn-analyses/{reference}/output/Reports/Quality_Metrics.csv"
            quality_items = lookup_file_by_path(quality_path)

            if quality_items:
                quality_file_id = quality_items[0]["data"]["id"]
                raw_bytes = download_csv_bytes(quality_file_id)
                out_bytes = add_id_library(raw_bytes, id_library)

                if out_bytes is not None:
                    qs_s3_key = f"{object_path_qs}/{quality_file_id}/Quality_Metrics.csv"
                    s3.load_bytes(bytes_data=out_bytes, key=qs_s3_key, bucket_name=bucket_name, replace=True)
                    logger.info(f"[S3] Uploaded Quality_Metrics → s3://{bucket_name}/{qs_s3_key}")
                    status_row["quality"] = "uploaded"
                    summary["quality_uploaded"] += 1
                else:
                    logger.info(f"[ICA] Quality_Metrics.csv empty for {reference}, skipping upload")
                    status_row["quality"] = "empty_skipped"
            else:
                logger.info(f"[ICA] Quality_Metrics.csv not found for {reference}")

        except Exception as e:
            logger.error(f"[ICA] Error processing Quality_Metrics for {reference}: {e}", exc_info=True)
            status_row["quality"] = "error"

        summary["analyses_processed"] += 1
        summary["per_analysis"].append(status_row)

    logger.info(
        f"[DONE] considered={summary['analyses_considered']}, "
        f"processed={summary['analyses_processed']}, "
        f"demux_uploaded={summary['demux_uploaded']}, "
        f"quality_uploaded={summary['quality_uploaded']}"
    )
    return summary
