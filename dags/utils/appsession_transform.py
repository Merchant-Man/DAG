import pandas as pd
import numpy as np

def transform_appsession_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    df = df.copy().drop_duplicates()

    # Source -> DB names (no created_at/updated_at, no biosample_name)
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
        "BioSampleName": "id_repository",          # <-- your PK
        "BioSampleId": "biosample_id",
        "ComputedYieldBps": "computed_yield_bps",
        "GeneratedSampleId": "generated_sample_id",
        "Yield": "yield",
        "TotalFlowcellYield": "total_flowcell_yield",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # Ensure PK exists; drop rows without id_repository (e.g., Run rows with no sample)
    if "id_repository" not in df.columns:
        df["id_repository"] = None
    df = df[df["id_repository"].notna() & (df["id_repository"] != "")]

    # Whitelist exactly the placeholders used by the SQL (no biosample_name, no timestamps)
    target_cols = [
        "id_repository",
        "row_type", "session_id", "session_name", "date_created", "date_modified",
        "execution_status", "ica_link", "ica_project_id", "workflow_reference",
        "run_id", "run_name", "percent_gt_q30", "flowcell_barcode", "reagent_barcode",
        "status", "experiment_name",
        "run_date_created",
        "biosample_id",
        "computed_yield_bps", "generated_sample_id",
        "yield", "total_flowcell_yield",
    ]
    for c in target_cols:
        if c not in df.columns:
            df[c] = None

    # Keep NULLs as None (don’t cast to str; don’t fill with "")
    df.replace({np.nan: None, "nan": None, "NaT": None, "": None}, inplace=True)

    # (Optional) strip tz markers from date strings your SQL won’t parse itself
    def _strip_tz(val):
        if val is None: return None
        s = str(val).replace("T", " ").replace("Z", "")
        if "+" in s: s = s.split("+", 1)[0]
        return s
    for c in ("date_created", "date_modified"):
        if c in df.columns:
            df[c] = df[c].map(_strip_tz)

    return df[target_cols]

