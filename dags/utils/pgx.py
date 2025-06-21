from airflow.models import Connection
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
import pandas as pd
import json
from sqlalchemy import create_engine, VARCHAR, SMALLINT
from botocore.client import BaseClient
from concurrent.futures import ThreadPoolExecutor, as_completed


def _get_pgx_report_json_files_from_s3(s3_client: BaseClient, bucket_name: str, key: str) -> pd.DataFrame:
    resp = s3_client.get_object(Bucket=bucket_name, Key=key)[
        "Body"].read().decode('utf-8')
    temp_report = json.loads(resp)

    order_id = temp_report.get("order_details").get("order_id")
    hubs = temp_report.get("order_details").get("ordered_by")
    test_method = temp_report.get("sample_details").get("test_method")
    version = temp_report.get("version", "")

    col = ["drug_name", "phenotype_text", "gene_symbol", "branded_drug", "drug_category", "drug_classification",
           "nala_score_v2", "rec_category", "rec_source", "rec_text", "run_id", "scientific_evidence_symbol", "implication_text"]

    temp_res = []

    for rec in temp_report.get("clinical_recommendations"):
        temp_res.append(
            (
                rec["drug_name"], rec["phenotype_text"],
                rec["gene_symbol"], rec["branded_drug"],
                rec["drug_category"], rec["drug_classification"],
                rec["nala_score_v2"], rec["rec_category"], rec["rec_source"], rec["rec_text"],
                rec["run_id"], rec["scientific_evidence_symbol"], rec["implication_text"]
            )
        )
    temp_df = pd.DataFrame(temp_res, columns=col)
    temp_df["order_id"] = order_id
    temp_df["hubs"] = hubs
    temp_df["test_method"] = test_method
    temp_df["version"] = version
    return temp_df


def get_pgx_summary(aws_conn_id: str, db_secret_url: str, **kwargs) -> None:
    """
    This function produces two outputs: aggregate of pgx recommendation for each drug-gene pair for each hub (stored inside superset_dev), and a detailed report of all recommendations for each order_id (stored inside dwh_restricted). 

    Parameters
    ---------
    db_secret_url: str
        The database secret url to connect to the database.
    """
    # Get desired report
    # Get all available order_id from RegINA
    pgx_report_bucket = "nl-data-pgx-output"
    query = """
    SELECT 
    DISTINCT report_path_ind path
    FROM (SELECT id_subject, report_path_ind, pgx_input_creation_date, ROW_NUMBER() OVER(PARTITION BY id_subject ORDER BY ind_report_creation_date) rn FROM gold_pgx_report WHERE ind_report_creation_date > "{date_filter}" AND report_path_ind IS NOT NULL
    ) t WHERE t.rn = 1 
    """
    query = query.format(pgx_report_bucket=pgx_report_bucket,
                         date_filter="2024-10-01" if kwargs.get("curr_ds") is None else kwargs.get("curr_ds"))
    engine = create_engine(db_secret_url)

    print(query)

    print(f" == Get PGx report list == ")
    with engine.connect() as conn:  # type: ignore
        pgx_reports = pd.read_sql(
            sql=query,
            con=conn.connection
        )
    
    if pgx_reports.empty:
        print("No PGx reports found for the specified date filter.")
        return

    pgx_reports = pgx_reports["path"].values.tolist()
    s3_client = boto3.client("s3",
                             aws_access_key_id=Connection.get_connection_from_secrets(
                                 aws_conn_id).login,
                             aws_secret_access_key=Connection.get_connection_from_secrets(
                                 aws_conn_id).password,
                             region_name="ap-southeast-3")

    # # Use ThreadPoolExecutor to process all subjects in parallel (adjust max_workers as needed)
    res = []

    print(f" == Get PGx report info from s3 == ")
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(_get_pgx_report_json_files_from_s3,
                                   s3_client, pgx_report_bucket, report) for report in pgx_reports]
        for future in as_completed(futures):
            res.append(future.result())
    res = pd.concat(res, ignore_index=True)
    # res.to_csv("pgx_report.csv", index=False)
    res.reset_index(drop=True, inplace=True)
    res.set_index(["order_id", "hubs", "drug_category",
                  "drug_name", "drug_classification"], inplace=True)

    print(f" == Save result to dwh == ")
    with engine.begin() as conn:
        res.to_sql(
            name=f"gold_pgx_detail_report",
            if_exists="replace",
            schema="dwh_restricted",
            con=conn,
            dtype={'hubs': VARCHAR(128), 'drug_name': VARCHAR(128), 'drug_category': VARCHAR(
                128), 'drug_classification': VARCHAR(128), 'gene_symbol': VARCHAR(16), 'order_id': VARCHAR(36)}
        )
    res.reset_index(inplace=True)
    grouped_res = res.groupby(["hubs", "drug_name", "gene_symbol", "rec_source", "scientific_evidence_symbol",
                              "nala_score_v2", "rec_category"]).size().reset_index(name="ct_subj_id")

    total_id_per_hubs = res.groupby(["hubs"]).agg(
        {"order_id": "nunique"}).reset_index()
    total_id_per_hubs.rename(
        columns={"order_id": "ct_total_subj_id"}, inplace=True)
    grouped_res = pd.merge(
        grouped_res, total_id_per_hubs, on="hubs", how="left")
    grouped_res.set_index(["hubs", "drug_name", "gene_symbol",
                          "nala_score_v2", "rec_category"], inplace=True)
    with engine.begin() as conn:
        grouped_res.to_sql(
            name=f"gold_pgx_summary_report",
            if_exists="replace",
            con=conn,
            dtype={"hubs": VARCHAR(128), "drug_name": VARCHAR(128), "gene_symbol": VARCHAR(
                16), "nala_score_v2": VARCHAR(32), "rec_category": SMALLINT}
        )
    print(f" == Finished == ")

    return
