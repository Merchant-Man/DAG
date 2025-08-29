from datetime import datetime
import re
from io import StringIO
from typing import Tuple
import pandas as pd
import boto3
import pandas as pd
import json
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine
import mysql.connector as sql
from botocore.client import BaseClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from utils.utils import dict_csv_buf_transform, extract_db_url


def extract_info(pd_row: pd.DataFrame, col) -> Tuple[str, str]:
    """
    Extracts id_repository and run_name from a column inside a dataframe row S3 path based on its bucket structure.

    - For paths containing "bgsi-data-citus-output":
        * run_name: the folder name immediately after the bucket.
        * id_repository: the portion of run_name before the underscore.

    - For paths containing "bgsi-data-illumina":
        * run_name: the substring (before the first dash) of the folder immediately following 'pro/analysis/'.
        * id_repository: the portion of run_name before the underscore.

    Returns a tuple (id_repository, run_name) or (None, None) if no pattern is matched.
    """
    run_name = ""
    id_repository = ""

    s3_path = pd_row[col]
    # Example s3://bgsi-data-citus-output-liquid/0E0050901C01_9fb9a9ea/0E0050901C01.cram/0E0050901C01.cram
    # s3://bgsi-data-dragen-output-liquid/0F0083701C01_e762c03d/0F0083701C01.cram
    if "bgsi-data-citus-output-liquid" in s3_path:
        pattern = r"bgsi-data-citus-output-liquid/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            long_run_name = match.group(1)
            run_name = long_run_name.split("-")[0]
    elif "bgsi-data-dragen-output-liquid/pro/analysis/" in s3_path:
        pattern = r"bgsi-data-dragen-output-liquid/pro/analysis/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            long_run_name = match.group(1)
            run_name = long_run_name.split("-")[0]
    elif "bgsi-data-dragen-output-liquid" in s3_path:
        pattern = r"bgsi-data-dragen-output-liquid/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            long_run_name = match.group(1)
            run_name = long_run_name.split("-")[0]
    elif "bgsi-data-citus-output" in s3_path:
        pattern = r"bgsi-data-citus-output/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            run_name = match.group(1)
            id_repository = run_name.split("_")[0]
    elif "bgsi-data-wfhv-output" in s3_path:
        pattern = r"bgsi-data-wfhv-output/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            run_name = match.group(1)
            id_repository = run_name.split("_")[0]
    # s3://bgsi-data-illumina/pro/analysis/0H0124401C01_5e62498f_rerun_2024-08-13_012704-DRAGEN_Germline_WGS_4-2-7_sw-mode-JK-eb5da97e-ea84-416d-8e3e-eeb7733f49fb/0H0124401C01/0H0124401C01.cram
    elif "bgsi-data-illumina" in s3_path:
        pattern = r"bgsi-data-illumina/pro/analysis/([^/]+)/"
        match = re.search(pattern, s3_path)
        if match:
            long_run_name = match.group(1)
            run_name = long_run_name.split("-")[0]

    if re.match(r"SKI_.*", run_name):
        # SKI_3175140_9efb681c-DRAGEN_Germline_WGS_4-2-7_sw-mode-JK-8b97ceb8-ab78-489a-8d48-901f1b240c79
        id_repository = re.search(r"SKI_[^_]+", run_name)[0]  # type: ignore
    elif re.match(r"[^_]+_[\d]{6}_M_.*", run_name):
        # 0C0123801C03_250209_M_1a75a295-ede8212c-9b83-4029-bade-7c19493b2fe7
        id_repository = re.search(
            r"[^_]+_[\d]{6}_[TM]{1}", run_name)[0]  # type: ignore
    elif re.match(r"[^_-]+-1-DRAGEN-4-2-6-Germline.*", run_name):
        # 0H0066801C01-1-DRAGEN-4-2-6-Germline-All-Callers-DRAGEN_Germline_WGS_4-2-6-v2_sw-mode-JK-b0a743a0-4762-436d-a988-4cd2be474910
        id_repository = re.search(r"[^_-]+", run_name)[0]  # type: ignore
    else:
        # 0C0067101C03_8b688247-DRAGEN_Germline_WGS_4-2-7_sw-mode-JK-df9ac94b-dbe8-4eb9-b377-6b0946661c4a
        id_repository = run_name.split('_')[0]

    return id_repository, run_name


def _get_report_info(pd_row: pd.DataFrame, col: str, client: BaseClient, bucket_name: str, key: str, report_type=["ind", "eng"]) -> Tuple[str, str, str, str]:
    """
    Constructs the S3 URL for a report based on sample_id and report_type ("ind" or 'eng').
    """
    res = []
    sample_id = pd_row[col]
    for lang in report_type:
        temp = key
        file_key = temp.format(sample_id=sample_id, report_type=lang)
        try:
            response = client.head_object(Bucket=bucket_name, Key=file_key)
            date_modified = response['LastModified']
            s3_path = f"s3://{bucket_name}/{file_key}"
            res.extend([s3_path, date_modified])
        except ClientError as e:
            res.extend(["", ""])

    # Get pipeline information from one of the json files, since the version is the same for both languages.
    try:
        response = client.get_object(Bucket=bucket_name, Key=key.format(
            sample_id=sample_id, report_type=report_type[0]))
        data = response['Body'].read().decode('utf-8')
        data = json.loads(data)
        version = data.get("version", "")
        res.extend([version])
    except ClientError as e:
        res.extend([""])

    # Ensure the result is always a tuple of four elements
    while len(res) < 5:
        res.append("")
    return tuple(res[:5])


def _fix_id_repository(df: pd.DataFrame, query: str, db_secret_url: str) -> pd.DataFrame:
    """
    Fix the id_repository in the DataFrame df by querying the database with the provided query.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe containing the id_repository column to be fixed.
    query: str 
        Query to fetch the id_repository and new_repository from the database.
    db_secret_url: str 
        Secret to connect to the database.

    Returns
    -------
    pd.DataFrame
        DataFrame with the id_repository fixed based on the query results.
    """

    if "id_repository" not in df:
        raise ValueError(
            "The DataFrame df must contain 'id_repository' column.")

    engine = create_engine(db_secret_url)

    print(f" == Get PGx report list == ")
    with engine.connect() as conn:  # type: ignore
        df_fix_id_repo = pd.read_sql(
            sql=query,
            con=conn.connection
        )

    if "id_repository" not in df_fix_id_repo.columns or "new_repository" not in df_fix_id_repo.columns:
        raise ValueError(
            "The DataFrame df_fix_id_repo must contain 'id_repository' and 'new_repository' columns. Please fix your query from DWH to include these columns.")

    print("Fixing id_repository")
    # https://stackoverflow.com/questions/50649853/trying-to-merge-2-dataframes-but-get-valueerror
    df = df.merge(df_fix_id_repo, on="id_repository",
                  how="left", suffixes=('', '_right'))

    df["temp_id_repository"] = df.new_repository.combine_first(
        df.id_repository)
    df.drop(columns=["id_repository", "new_repository"], inplace=True)
    df.rename(columns={"temp_id_repository": "id_repository"}, inplace=True)

    return df


def get_pgx_report_and_dump(input_bucket_name: str, output_bucket_name: str, dwh_bucket_name: str, object_path: str, db_secret_url: str, aws_conn_id: str, output_path_template: str, **kwargs) -> None:
    """
    Get PGx report from s3 and dump it into a single csv inside bronze bucket
    """

    s3_client = boto3.client("s3",
                             aws_access_key_id=Connection.get_connection_from_secrets(
                                 aws_conn_id).login,
                             aws_secret_access_key=Connection.get_connection_from_secrets(
                                 aws_conn_id).password,
                             region_name="ap-southeast-3")

    # ================ Get .csv files for PGx input ================
    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {'Bucket': input_bucket_name}
    page_iterator = paginator.paginate(**operation_parameters)

    # Get .csv filles for PGx input
    print("Get PGx input files from S3")
    cur_datetime = datetime.strptime(kwargs.get("curr_ds") or "2023-01-01", "%Y-%m-%d")
    print(cur_datetime)
    files = []
    for page in page_iterator:
        # Page object returns: ResponseMetadata and the response Contentents.
        # Example: {'Key': 'foo.csv', 'LastModified': datetime.datetime(2025, 2, 5, 14, 0, 36, tzinfo=tzutc()), 'ETag': '"123455"', 'ChecksumAlgorithm': ['CRC64NVME'], 'Size': 608, 'StorageClass': 'STANDARD'}
        files.extend([obj for obj in page.get('Contents', [])
                      # LastModified boto3 contains tzlocal() which chould be removed. tzinfo of airflow (utc) and s3 (ap-southeast-3) are different
                      # https://stackoverflow.com/questions/13218506/how-to-get-system-timezone-setting-and-pass-it-to-pytz-timezone
                     if (obj["Key"].endswith(".csv") and (obj["LastModified"].replace(tzinfo = None) >= cur_datetime ) )]) 

    print(f"Find {len(files)} files.")

    # No file found
    if not files:
        print("Returning due to no files found")
        return
    
    # Read csv for each object
    print("Get input information from sample sheets")
    df = pd.DataFrame()
    for i, file in enumerate(files):
        if i % 100 == 0:
            print(f"Processing file {i} of {len(files)}: {file['Key']}")
        # Get object, parse the body string, and decode into utf-8 (i.e. the '\n' newline), and concat into pandas.
        temp_df = pd.read_csv(StringIO(s3_client.get_object(
            Bucket=input_bucket_name, Key=file["Key"])["Body"].read().decode('utf-8')))
        temp_df["file_name"] = file["Key"].split(".")[0]
        temp_df["input_creation_date"] = file["LastModified"]

        temp_df[["id_repository", "run_name"]] = temp_df.apply(
            extract_info, col="bam", axis=1, result_type="expand")

        df = pd.concat([df, temp_df], ignore_index=True)
    print("Finished get input metadata")

    # ================ Get .csv files for PGx Output ================
    print("Get output metadata for each input which taken from the sample sheets")
    df[["report_path_ind", "ind_report_creation_date", "report_path_eng", "eng_report_creation_date", "pipeline_version"]] = df.apply(
        _get_report_info, col="sample_id", client=s3_client, bucket_name=output_bucket_name, key=output_path_template, axis=1, result_type="expand")
    s3_client.close()

    df = df.loc[:, ["file_name", "bam", "input_creation_date", "id_repository", "hub_name", "run_name",
                    "report_path_ind", "ind_report_creation_date", "report_path_eng", "eng_report_creation_date", "pipeline_version"]]
    df.astype(str)
    df.fillna(value="", inplace=True)
    print("Finished get output metadata")

    # Fix id_repository since sometimes input valeus are broken from the sample sheets.
    print("Get id_repository fix from DWH")
    query = """
        SELECT DISTINCT id_repository, new_repository FROM superset_dev.dynamodb_fix_id_repository_latest;
    """
    df = _fix_id_repository(df, query, db_secret_url)

    # Fix SKI id_repository from DWH
    print("Get id_repository SKI fix from DWH")
    query = """
        SELECT DISTINCT new_origin_code_repository id_repository, code_repository new_repository FROM staging_fix_ski_id_repo;
    """
    df = _fix_id_repository(df, query, db_secret_url)
    print("Finished fixing id_repository")

    df = dict_csv_buf_transform(df.to_dict(orient="records"))

    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"

    print(f"Dumping PGx report to {file_name} in S3 bucket {dwh_bucket_name}")
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=df,
        key=file_name,
        bucket_name=dwh_bucket_name,
        replace=True
    )


def transform_pgx_logs_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    df = df[["file_name", "bam", "input_creation_date", "id_repository", "hub_name", "run_name", "report_path_ind",
             "ind_report_creation_date", "report_path_eng", "eng_report_creation_date", "pipeline_version"]]

    # Convert date_start to datetime for sorting
    df["input_creation_date"] = pd.to_datetime(df["input_creation_date"])
    df["ind_report_creation_date"] = pd.to_datetime(
        df["ind_report_creation_date"])
    df["eng_report_creation_date"] = pd.to_datetime(
        df["eng_report_creation_date"])
    df.sort_values(by=['input_creation_date',
                   'ind_report_creation_date'], ascending=False, inplace=True)

    # Filter only either input_creation_date or ind_report_creation_date is yesterday.

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    print(df.dtypes)
    return df


def _get_pgx_report_json_files_from_s3(s3_client: BaseClient, bucket_name: str, key: str) -> pd.DataFrame:
    try:
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
    except ClientError as e:
        print(f"Error fetching {key} from bucket {bucket_name}: {e}")
        return pd.DataFrame()


def get_pgx_detail_to_dwh(aws_conn_id: str, pgx_report_output_bucket:str, db_secret_url: str, **kwargs) -> None:
    """
    This function produces two outputs: aggregate of pgx recommendation for each drug-gene pair for each hub (stored inside superset_dev), and a detailed report of all recommendations for each order_id (stored inside dwh_restricted). 

    Parameters
    ---------
    db_secret_url: str
        The database secret url to connect to the database.
    """
    # Get desired report
    # Get all available order_id from RegINA
    query = """
    SELECT DISTINCT
        REGEXP_REPLACE(report_path_ind, "s3://nl-data-pgx-output/", "") path
    FROM
        (
            SELECT
                report_path_ind
            FROM
                staging_pgx_report_status
            WHERE
                ind_report_creation_date > "{date_filter}"
                AND (report_path_ind IS NOT NULL OR report_path_ind != "" OR report_path_ind != "nan")
        ) t
    """
    query = query.format(date_filter="2024-10-01" if kwargs.get("curr_ds") is None else kwargs.get("curr_ds"))
    engine = create_engine(db_secret_url)

    print(query)
    print(db_secret_url)
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
                                   s3_client, pgx_report_output_bucket, report) for report in pgx_reports]
        for future in as_completed(futures):
            res.append(future.result())
    print(res[0])
    df = pd.concat(res, ignore_index=True)
    res = None
    df.reset_index(drop=True, inplace=True)

    df = df[["order_id","hubs","drug_category","drug_name","drug_classification","phenotype_text","gene_symbol","branded_drug","nala_score_v2","rec_category","rec_source","rec_text","run_id","scientific_evidence_symbol","implication_text","test_method","version"]]

    print(f"Data columns: {df.columns}")
    user, passwd, host, port, db = extract_db_url(db_secret_url)

    conn = sql.connect(db=db, user=user, host=host,
                       password=passwd, use_unicode=True)
    cur = conn.cursor()
    insert_query = kwargs["templates_dict"]["insert_query"]
    print("====START INSERTING====")
    print(f"Data Points: {df.shape[0]}")
    print(f"QUERY:\n{insert_query}")
    chunk_size = 2000
    for start in range(0, df.shape[0], chunk_size):
        chunk = df[start:start + chunk_size]
        print(f"Chunk data Points: {chunk.shape[0]}")

        print(f"Chunk data:\n{chunk.head()}")
        data = list(chunk.itertuples(index=False, name=None))
        try:
            # print(data)
            cur.executemany(insert_query, data)
            conn.commit()

        except (sql.Error, sql.Warning) as e:
            conn.close()
            raise ValueError(e)
    print("====FINISHED====")
    return
