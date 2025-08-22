import pandas as pd
import numpy as np

def transform_appsession_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    df = df.copy().drop_duplicates()

    # Map source -> DB columns
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
        "BioSampleName": "id_repository",        # PK
        "BioSampleId": "biosample_id",
        "ComputedYieldBps": "computed_yield_bps",
        "GeneratedSampleId": "generated_sample_id",
        "Yield": "yield",
        "TotalFlowcellYield": "total_flowcell_yield",
        # DO NOT include created_at/updated_at (SQL sets NOW(6))
    }

    # If both exist, prefer an already-present id_repository, else fallback to BioSampleName
    if "id_repository" in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["id_repository"].where(
            df["id_repository"].notna() & (df["id_repository"] != ""),
            df["BioSampleName"]
        )
        df.drop(columns=["BioSampleName"], inplace=True)

    # Rename what we have
    rename_map_effective = {k: v for k, v in rename_map.items() if k in df.columns}
    df.rename(columns=rename_map_effective, inplace=True)

    # Keep only the placeholders used by the INSERT (no timestamps)
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

    # Normalize nulls (let MySQL receive NULL, not "")
    df.replace({np.nan: None, "nan": None, "NaT": None, "": None}, inplace=True)

    # Optional: strip timezone markers in date_created/date_modified if they exist
    def _strip_tz(val):
        if val is None:
            return None
        s = str(val)
        s = s.replace("T", " ").replace("Z", "")
        # drop any +HH:MM or -HH:MM suffix
        if "+" in s:
            s = s.split("+", 1)[0]
        if "-" in s and s.count("-") > 2 and s.endswith((":00", ":30", ":45")):
            # crude handle for trailing -HH:MM offsets
            s = s.rsplit("-", 1)[0]
        return s

    for c in ("date_created", "date_modified"):
        if c in df.columns:
            df[c] = df[c].map(_strip_tz)

    # IMPORTANT: do NOT cast to str; keep types so ints stay ints and NULLs stay None
    df = df[target_cols]

    return df
