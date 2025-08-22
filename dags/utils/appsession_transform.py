import pandas as pd
import numpy as np

def transform_appsession_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().drop_duplicates()

    # --- Coalesce BioSampleName → id_repository (PK) ---
    if "id_repository" not in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["BioSampleName"]
    elif "id_repository" in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["id_repository"].where(
            df["id_repository"].notna() & (df["id_repository"] != ""), df["BioSampleName"]
        )

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

    # Drop non-sample "Run" rows (no PK)
    df = df[df["id_repository"].notna() & (df["id_repository"] != "")]

    # Optional: trim timezone artifacts for date/time strings (SQL parses run_date_created)
    def _strip_tz(x):
        if pd.isna(x): return None
        s = str(x).replace("T"," ").replace("Z","")
        if "+" in s: s = s.split("+", 1)[0]
        return s
    for c in ("date_created", "date_modified"):
        if c in df.columns:
            df[c] = df[c].map(_strip_tz)

    # Ensure numeric types where helpful
    numeric_cols = [
        "percent_gt_q30", "computed_yield_bps", "yield", "total_flowcell_yield",
        "reads_total", "reads_perfect_index", "reads_one_mismatch_index", "reads_two_mismatch_index", "reads_percent",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Keep exactly what the SQL expects, in order (no created_at/updated_at; INSERT uses NOW(6))
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
