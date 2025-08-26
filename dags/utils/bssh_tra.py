# import pandas as pd

# def transform_appsessions_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
#     # Remove duplicates
#     df = df.drop_duplicates()
#     cols = {
#     'row_type': df['RowType']
#     , 'session_id': df['SessionId']
#     , 'session_name': df['SessionName']
#     , 'date_created': df['DateCreated']
#     , 'date_modified': df['DateModified']
#     , 'execution_status': df['ExecutionStatus']
#     , 'ica_link': df['ICA_Link']
#     , 'ica_project_id': df['ICA_ProjectId']
#     , 'workflow_reference': df['WorkflowReference']
#     , 'run_id': df['RunId']
#     , 'run_name': df['RunName']
#     , 'percent_gt_q30': df['PercentGtQ30']
#     , 'flowcell_barcode': df['FlowcellBarcode']
#     , 'reagent_barcode': df['ReagentBarcode']
#     , 'status': df['Status']
#     , 'experiment_name': df['ExperimentName']
#     , 'run_date_created': df['RunDateCreated']
#     , 'id_repository': df['BioSampleName']
#     , 'biosample_id': df['BioSampleId']
#     , 'computed_yield_bps': df['ComputedYieldBps']
#     , 'generated_sample_id': df['GeneratedSampleId']
#     }

#     # Construct the new DataFrame
#     df = pd.DataFrame(cols)
    
#     if "created_at" not in df.columns:
#         df["created_at"] = ts
#     if "updated_at" not in df.columns:
#         df["updated_at"] = ts
#     # Need to fillna so that the mysql connector can insert the data.
#     df = df.astype(str)
#     df.fillna(value="", inplace=True)
#     return df