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
        # created_at / updated_at handled below
    }

    # If both exist, coalesce into id_repository then drop BioSampleName
    if "id_repository" in df.columns and "BioSampleName" in df.columns:
        df["id_repository"] = df["id_repository"].where(
            df["id_repository"].notna() & (df["id_repository"] != ""),
            df["BioSampleName"]
        )
        df.drop(columns=["BioSampleName"], inplace=True)
    else:
        # Rename only for columns that are present
        df.rename(columns={k: v for k, v in cols.items() if k in df.columns}, inplace=True)

    # Ensure all mapped target columns exist (create empty if missing)
    for target in cols.values():
        if target not in df.columns:
            df[target] = ""

    # Ensure timestamps
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Cast to str and fill na for MySQL
    df = df.astype(str)
    df.fillna("", inplace=True)

    # Keep only mapped + timestamps, in stable order
    new_cols = list(cols.values()) + ["created_at", "updated_at"]
    df = df[new_cols]

    return df

    
