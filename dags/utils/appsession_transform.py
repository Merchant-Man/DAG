import pandas as pd

def transform_appsession_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    df = df.copy().drop_duplicates()

    # Desired rename mapping (normalize caps + BioSampleName -> id_repository)
    cols = {
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
        "BioSampleName": "id_repository",          # <- key requirement
        "BioSampleId": "biosample_id",
        "ComputedYieldBps": "computed_yield_bps",
        "GeneratedSampleId": "generated_sample_id",
        "Yield": "yield",
        "TotalFlowcellYield": "total_flowcell_yield",
        "created_at": "created_at",
        "updated_at": "updated_at",
    }

    # If both exist, coalesce into id_repository, then drop BioSampleName
    if "id_repository" in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["id_repository"].where(
            df["id_repository"].notna() & (df["id_repository"] != ""),
            df["BioSampleName"]
        )
        df.drop(columns=["BioSampleName"], inplace=True)

    # Rename all other present columns per mapping (BioSampleName is already handled)
    rename_map = {k: v for k, v in cols.items() if k in df.columns and k != "BioSampleName"}
    df.rename(columns=rename_map, inplace=True)

    # Ensure timestamps exist
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Ensure all target columns exist (create empty if missing)
    target_cols = list(cols.values())
    for c in target_cols:
        if c not in df.columns:
            df[c] = ""

    # Cast to str and fill na for MySQL
    df = df.astype(str)
    df.fillna("", inplace=True)

    # Keep only mapped columns in stable order (no duplicates)
    df = df[target_cols]

    return df
