import pandas as pd
import numpy as np
import re

# Columns your current INSERT uses, in order (per your logs)
LOADER_COLS = [
    "row_type","session_id","session_name","date_created","date_modified",
    "execution_status","ica_link","ica_project_id","workflow_reference",
    "run_id","run_name","percent_gt_q30","flowcell_barcode","reagent_barcode",
    "status","experiment_name","run_date_created","id_repository","biosample_id",
    "computed_yield_bps","generated_sample_id","created_at","updated_at",
]

def transform_appsession_data(df: pd.DataFrame, *_args, **_kwargs) -> pd.DataFrame:
    df = df.copy()

    # --- 1) Normalize headers: collapse spaces, strip ---
    def _collapse_spaces(s: str) -> str:
        return re.sub(r"\s+", " ", str(s)).strip()

    df.columns = pd.Index(df.columns).map(_collapse_spaces)

    # Common alias fixes (spaces vs underscores, etc.)
    alias = {
        "ICA Link": "ICA_Link",
        "ICA ProjectId": "ICA_ProjectId",
        "Workflow Reference": "WorkflowReference",
        "Flowcell Barcode": "FlowcellBarcode",
        "Reagent Barcode": "ReagentBarcode",
        "Experiment Name": "ExperimentName",
        "Run Date Created": "RunDateCreated",
        "Bio Sample Name": "BioSampleName",
        "Bio Sample Id": "BioSampleId",
        "Computed Yield Bps": "ComputedYieldBps",
        "Generated Sample Id": "GeneratedSampleId",
        "Total Flowcell Yield": "TotalFlowcellYield",
    }
    df.rename(columns={k: v for k, v in alias.items() if k in df.columns}, inplace=True)

    # Compact the metric headers to your requested forms
    compact_metric_map = {
        "# Reads": "#Reads",
        "# Perfect Index Reads": "#PerfectIndexReads",
        "# One Mismatch Index Reads": "#OneMismatchIndexReads",
        "# Two Mismatch Index Reads": "#TwoMismatchIndexReads",
        "% Reads": "%Reads",
        "Yield": "Yield",
        "TotalFlowcellYield": "TotalFlowcellYield",
    }
    ren = {}
    for c in df.columns:
        norm = _collapse_spaces(c)
        if norm in compact_metric_map:
            ren[c] = compact_metric_map[norm]
    if ren:
        df.rename(columns=ren, inplace=True)

    # --- 2) Clean string-like values; turn blanks/NAN/NULL text into real NULLs ---
    def _clean(v):
        if pd.isna(v):
            return None
        s = str(v).strip()
        return None if s.lower() in {"", "nan", "none", "null"} else s

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].map(_clean)

    df.drop_duplicates(inplace=True)

    # --- 3) Rename core columns to snake_case expected by SQL ---
    rename_map = {
        "RowType": "row_type",
        "SessionId": "session_id",
        "SessionName": "session_name",
        "DateCreated": "date_created",
        "DateModified": "date_modified",
        "ExecutionStatus": "execution_status",
        "ICA_Link": "ica_link",
        "ICA_ProjectId": "ica_project_id",
        "WorkflowReference": "workflow_reference",
        "RunId": "run_id",
        "RunName": "run_name",
        "PercentGtQ30": "percent_gt_q30",
        "FlowcellBarcode": "flowcell_barcode",
        "ReagentBarcode": "reagent_barcode",
        "Status": "status",
        "ExperimentName": "experiment_name",
        "RunDateCreated": "run_date_created",
        "BioSampleId": "biosample_id",
        "ComputedYieldBps": "computed_yield_bps",
        "GeneratedSampleId": "generated_sample_id",
        # keep the human headers "Yield"/"TotalFlowcellYield" but also offer snake_case copies:
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # --- 4) Keep only BioSample rows (case/space tolerant) ---
    if "row_type" not in df.columns:
        return pd.DataFrame(columns=LOADER_COLS)

    rt_norm = (
        df["row_type"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.casefold()
    )
    df = df[rt_norm == "biosample"].copy()
    if df.empty:
        return pd.DataFrame(columns=LOADER_COLS)

    # Normalize display of row_type
    df["row_type"] = "BioSample"

    # --- 5) PK: id_repository from BioSampleName ---
    if "id_repository" not in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["BioSampleName"]
    elif "id_repository" in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["id_repository"].where(df["id_repository"].notna(), df["BioSampleName"])

    # Drop rows without a usable PK
    df = df[df["id_repository"].notna() & (df["id_repository"] != "")]

    # --- 6) Parse dates: make MySQL-friendly (naive) ---
    def _strip_tz_text(x):
        if x is None:
            return None
        s = str(x).replace("T", " ").replace("Z", "")
        if "+" in s:
            s = s.split("+", 1)[0]
        return s

    if "run_date_created" in df.columns:
        df["run_date_created"] = (
            pd.to_datetime(df["run_date_created"].map(_strip_tz_text), errors="coerce", utc=True)
            .dt.tz_convert(None)
        )
    for c in ("date_created", "date_modified"):
        if c in df.columns:
            df[c] = df[c].map(_strip_tz_text)

    # --- 7) Numeric casts used by loader; also compute reads/yield metrics ---
    for c in ("percent_gt_q30", "computed_yield_bps"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Source helper (compact name first, then fallbacks)
    def src(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    # Reads / index metrics
    c = src("#Reads", "# Reads")
    if c: df["reads_total"] = pd.to_numeric(df[c], errors="coerce")

    a = src("#PerfectIndexReads", "# Perfect Index Reads", "# Perfect Index Index Reads")
    if a: df["reads_perfect_index"] = pd.to_numeric(df[a], errors="coerce")

    b = src("#OneMismatchIndexReads", "# One Mismatch Index Reads")
    if b: df["reads_one_mismatch_index"] = pd.to_numeric(df[b], errors="coerce")

    d = src("#TwoMismatchIndexReads", "# Two Mismatch Index Reads")
    if d: df["reads_two_mismatch_index"] = pd.to_numeric(df[d], errors="coerce")

    e = src("%Reads", "% Reads")
    if e: df["reads_percent"] = pd.to_numeric(df[e], errors="coerce")

    # ✅ Yield metrics (both the compact display and snake_case copies for future loaders)
    if "Yield" in df.columns:
        df["Yield"] = pd.to_numeric(df["Yield"], errors="coerce")
        df["yield"] = df["Yield"]  # snake_case mirror (not used by current INSERT)

    if "TotalFlowcellYield" in df.columns:
        df["TotalFlowcellYield"] = pd.to_numeric(df["TotalFlowcellYield"], errors="coerce")
        df["total_flowcell_yield"] = df["TotalFlowcellYield"]  # snake_case mirror

    # --- 8) Ensure loader columns exist + timestamps ---
    now_str = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    for c in LOADER_COLS:
        if c not in df.columns:
            df[c] = None

    if df["created_at"].isna().all():
        df["created_at"] = now_str
    if df["updated_at"].isna().all():
        df["updated_at"] = now_str

    # Final NULL normalization
    df.replace({np.nan: None, "NaT": None}, inplace=True)

    # --- 9) Return EXACTLY what the loader expects (order matters for %s placeholders) ---
    return df[LOADER_COLS]
