import pandas as pd
import numpy as np
import re

def transform_appsession_data(df: pd.DataFrame, *_args, **_kwargs) -> pd.DataFrame:
    df = df.copy()

    # --- Normalize headers early: trim, collapse whitespace, fix common variants ---
    df.columns = (
        pd.Index(df.columns)
        .map(lambda x: str(x))
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    # Common alias fixes (spaces vs underscores, trailing spaces, etc.)
    alias_fix = {
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
        "# Reads ": "# Reads",
        "# Perfect Index Reads ": "# Perfect Index Reads",
        "# One Mismatch Index Reads ": "# One Mismatch Index Reads",
        "# Two Mismatch Index Reads ": "# Two Mismatch Index Reads",
        "% Reads ": "% Reads",
    }
    df.rename(columns={k: v for k, v in alias_fix.items() if k in df.columns}, inplace=True)

    # Trim string cells (object cols) to deal with stray spaces in values
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip().replace({"": None, "nan": None, "NaN": None})

    # Drop exact dupes after header/value normalization
    df = df.drop_duplicates()

    # --- Coalesce BioSampleName → id_repository (PK) ---
    if "id_repository" not in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["BioSampleName"]
    elif "id_repository" in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["id_repository"].where(
            df["id_repository"].notna() & (df["id_repository"] != ""), df["BioSampleName"]
        )
    # Final trim on id_repository
    if "id_repository" in df.columns:
        df["id_repository"] = df["id_repository"].astype(str).str.strip().replace({"": None})

    # --- Unify the two "Perfect Index" spellings ---
    if "# Perfect Index Reads" in df.columns or "# Perfect Index Index Reads" in df.columns:
        a = df.get("# Perfect Index Reads")
        b = df.get("# Perfect Index Index Reads")
        df["reads_perfect_index"] = pd.to_numeric(a, errors="coerce").fillna(pd.to_numeric(b, errors="coerce"))

    # Other read metrics
    if "# Reads" in df.columns:
        df["reads_total"] = pd.to_numeric(df["# Reads"], errors="coerce")
    if "# One Mismatch Index Reads" in df.columns:
        df["reads_one_mismatch_index"] = pd.to_numeric(df["# One Mismatch Index Reads"], errors="coerce")
    if "# Two Mismatch Index Reads" in df.columns:
        df["reads_two_mismatch_index"] = pd.to_numeric(df["# Two Mismatch Index Reads"], errors="coerce")
    if "% Reads" in df.columns:
        df["reads_percent"] = pd.to_numeric(df["% Reads"], errors="coerce")

    # --- Normalize core columns ---
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
        "Yield": "yield",
        "TotalFlowcellYield": "total_flowcell_yield",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # Keep only rows that have a PK
    if "id_repository" in df.columns:
        df = df[df["id_repository"].notna() & (df["id_repository"] != "")]
    else:
        # If we somehow still don't have it, return empty with target schema to avoid insert errors
        df = df.iloc[0:0]

    # Strip timezone artifacts for date/time strings (SQL converts only run_date_created)
    def _strip_tz(x):
        if pd.isna(x):
            return None
        s = str(x).replace("T", " ").replace("Z", "")
        if "+" in s:
            s = s.split("+", 1)[0]
        return s

    for c in ("date_created", "date_modified"):
        if c in df.columns:
            df[c] = df[c].map(_strip_tz)

    # Ensure numeric types
    numeric_cols = [
        "percent_gt_q30", "computed_yield_bps", "yield", "total_flowcell_yield",
        "reads_total", "reads_perfect_index", "reads_one_mismatch_index", "reads_two_mismatch_index", "reads_percent",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Final target shape (created_at/updated_at are DB-managed)
    target_cols = [
        "id_repository",
        "row_type","session_id","session_name","date_created","date_modified",
        "execution_status","ica_link","ica_project_id","workflow_reference",
        "run_id","run_name","percent_gt_q30","flowcell_barcode","reagent_barcode",
        "status","experiment_name",
        "run_date_created",
        "biosample_id",
        "computed_yield_bps","generated_sample_id",
        "yield","total_flowcell_yield",
        "reads_total","reads_perfect_index","reads_one_mismatch_index","reads_two_mismatch_index","reads_percent",
    ]
    for c in target_cols:
        if c not in df.columns:
            df[c] = None

    # Convert NaN/NaT/"" to None for MySQL NULLs
    df.replace({np.nan: None, "nan": None, "NaT": None, "": None}, inplace=True)

    return df[target_cols]
