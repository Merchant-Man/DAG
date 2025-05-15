from typing import Callable, Optional, Dict, Any, List, Union
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from .utils import dict_csv_buf_transform, default_retry_args, extract_db_url
import json
import pandas as pd
import mysql.connector as sql
import time
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, DATETIME
import re
import ast


def transform_demography_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    df['gender'].replace({'female': 'FEMALE', 'male': 'MALE'}, inplace=True)

    def _get_hospital_name(creator_email: str) -> str:
        # Get hospital name from creator email
        if re.search(r"rspon", creator_email):
            return "RS PON Prof. Dr. dr. Mahar Mardjono"
        elif re.search(r"sardjitohospital", creator_email):
            return "RSUP Dr. Sardjito"
        elif re.search(r"rscm", creator_email):
            return "RS Dr. Cipto Mangunkusumo"
        elif re.search(r"rsabhk", creator_email):
            return "PKIAN RSAB Harapan Kita"
        elif re.search(r"dharmais", creator_email):
            return "RS Kanker Dharmais"
        elif re.search(r"pjnhk", creator_email):
            return "RSPJD Harapan Kita"
        elif re.search(r"sintayunita|admin", creator_email):
            return "BGSi Central"
        elif re.search(r"profngoerahhospitalbali", creator_email):
            return "RSUP Prof. Dr. IGNG Ngoerah Denpasar"
        elif re.search(r"rspisuliantisaroso", creator_email):
            return "RSPI Prof. Dr. Sulianti Saroso"
        elif re.search(r"rsuppersahabatan", creator_email):
            return "RSUP Persahabatan"
        else:
            return "Unknown"

    df['hospital_name'] = df['created_by'].apply(_get_hospital_name)

    df.rename(columns={'id': 'id_subject', 'created_at': 'creation_date',
                       'updated_at': 'updation_date', 'gender': 'sex', 'nik': 'encrypt_nik', 'full_name': 'encrypt_full_name', 'dob': 'encrypt_birth_date', 'ihs_number': 'encrypt_ihs_number'}, inplace=True)

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    df = df[["id_subject", "encrypt_full_name", "encrypt_nik", "encrypt_birth_date", "sex", "source",
             "province_code", "province", "district_code", "district", "subdistrict_code", "subdistrict", "village_code", "village", "use_nik_ibu", "created_by", "updated_by",  "hospital_name",  "created_at", "updated_at", "creation_date", "updation_date", "encrypt_ihs_number"]]

    print(df.head())
    print(df.columns)

    # Even we remove duplicates, API might contain duplicate records for an id_subject
    # So, we will keep the latest record
    df['updation_date'] = pd.to_datetime(df['updation_date'])
    df = df.sort_values('updation_date').groupby('id_subject').tail(1)

    df['updation_date'] = df["updation_date"].dt.strftime(
        '%Y-%m-%d %H:%M:%S').astype('str')

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df = df.fillna('')
    return df


def transform_variable_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()
    df = df.astype(str)
    return df


def fetch_documents_all_participants_and_dump(api_conn_id: str, db_secret_url: str, data_end_point: str, aws_conn_id: str, bucket_name: str,
                                              object_path: str, headers: Optional[Dict[str, Any]] = {},
                                              jwt_end_point: str = "", jwt_headers: Optional[Dict[str, Any]] = {},
                                              data_payload: Optional[Dict[str, Any]] = {
                                              },
                                              jwt_payload: Optional[Dict[str, Any]] = {
                                              },
                                              get_token_function: Optional[Callable[[
                                                  Dict[str, Any]], str]] = None,
                                              retry_args: Dict[str,
                                                               Any] = default_retry_args,
                                              response_key_data: Union[List[str], str] = "data", transform_func: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = dict_csv_buf_transform,
                                              **kwargs) -> bool:
    """
    Returns
    -------
    bool
        True if there is any data and successfully dumped.
    """

    print("=== Fetching Data ===")
    all_data = []

    user, passwd, host, port, db = extract_db_url(db_secret_url)
    conn = sql.connect(db=db, user=user, host=host,
                       password=passwd, use_unicode=True)

    id_repos = pd.read_sql_query(
        "SELECT id_subject FROM phenovar_participants", conn)["id_subject"].values
    conn.close()

    for idx in range(len(id_repos)):
        if idx % 500 == 0:
            access_token = {}
            # Need relogin for long fetching time
            if jwt_end_point and get_token_function and (jwt_headers or jwt_payload):
                print("=== Fetching JWT token ===")
                http_hook = HttpHook(method="POST", http_conn_id=api_conn_id)

                response = http_hook.run_with_advanced_retry(
                    endpoint=jwt_end_point,
                    headers=jwt_headers,
                    data=json.dumps(jwt_payload),
                    _retry_args=retry_args
                )
                try:
                    response_header = response.headers
                    response = response.json()
                    access_token = get_token_function(
                        response, response_header)
                except json.JSONDecodeError as e:
                    print(f"Decoding into JSON failed: {e}")
                    print(f"Failed response: {response}")
                    raise ValueError("Failed to decode the response")

            if access_token:
                if "headers" in access_token:
                    headers.update(access_token["headers"])
                elif "params" in access_token:
                    data_payload.update(access_token["params"])
                else:
                    raise ValueError(
                        "get_token_function doesn't return 'headers' or 'params'! which is required to be integrated for subsequence response to get the data.")
            http_hook = HttpHook(method="GET", http_conn_id=api_conn_id)

        req_end_point = data_end_point.format(id=id_repos[idx])
        response = http_hook.run_with_advanced_retry(
            endpoint=req_end_point,
            headers=headers,
            data=data_payload,
            _retry_args=retry_args
        )
        try:
            response_json = response.json()
            if response_key_data not in response_json:
                continue
            data = response_json.get(response_key_data)
            if not data:
                continue
            all_data.extend(data)

        except json.JSONDecodeError as e:
            print(f"Decoding into JSON failed: {e}")
            print(f"Failed response: {response}")
            continue
        # To prevent rate limiting :(((( please create a bulk request or better filter the last updated document via API. GOSSSHHH
        time.sleep(2)

    if not all_data:
        print("No data to update")
        return False

    if transform_func:
        all_data = transform_func(all_data)

    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=all_data,
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )
    return True


def transform_document_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()
    df["question_answer"] = df["question_answer"].apply(json.dumps)
    df["user"] = df["user"].apply(json.dumps)
    return df


def flatten_document_data(db_secret_url: str, verbose: bool = False) -> None:
    """
    Flatten Phenovar Document data

    Parameters
    ----------
    db_secret_url : str
        Database secret URL
    verbose : bool, optional
        Verbose mode, by default False
    """
    # Get all available id_subject from RegINA
    docs_query = r"""
    SELECT * FROM phenovar_documents
    """

    engine = create_engine(db_secret_url)

    with engine.connect() as conn:
        phenovar_docs_df = pd.read_sql(
            sql=docs_query,
            con=conn.connection
        )

    # Get variable id mapping to map column name
    variable_query = r"""
    SELECT * FROM phenovar_variables
    """
    with engine.connect() as conn:
        phenovar_var_df = pd.read_sql(
            sql=variable_query,
            con=conn.connection
        )
    variable_map = {}
    phenovar_var_df[["id", "name", "is_mandatory"]].apply(
        lambda row: variable_map.setdefault(row["id"], {"name": row["name"], "is_mandatory": row["is_mandatory"]}) if row["id"] not in variable_map else None, axis=1
    )

    # the question_answer column values are in the format of string JSON
    def _get_form_group(doc_type: str) -> str:
        match = re.findall(
            r"^AAA\d+\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\/([^|]+)\|(.+)", doc_type)
        if match:
            return "_".join(match[0])
    phenovar_docs_df["form_group"] = phenovar_docs_df["document_type"].apply(
        _get_form_group)

    unique_form_group = list(phenovar_docs_df["form_group"].unique())

    form_group_df_dict = {}
    for form_group in unique_form_group:
        if verbose:
            print(f"Processing form_group: {form_group}")
        # Skip if form_group is None
        if form_group == None:
            continue
        temp_df = phenovar_docs_df[phenovar_docs_df["form_group"]
                                   == form_group]
        # Make the column into dictionary
        temp_data = temp_df["question_answer"].apply(
            lambda x: json.loads(x)).apply(lambda x: ast.literal_eval(x))

        # After that, we can use Pandas functionalities to normalize dictionary data.
        temp_data_norm = pd.json_normalize(temp_data.to_list())

        # Excluding the continous values whose values are actually how to fill it which are not needed.
        # form.continue_values.uuid
        excl_cols = []
        for col in list(temp_data_norm.columns):
            if re.findall(r"form\.continue_values\.[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", col):
                excl_cols.append(col)
        temp_data_norm.drop(excl_cols, axis=1, inplace=True)

        new_cols = []
        for col in temp_data_norm.columns:
            temp_uuid = re.findall(
                r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", col)
            col = re.sub(rf"form\.[^.]+\.", "", col)
            if temp_uuid:
                name = variable_map[temp_uuid[0]]["name"]
                if name:
                    # strip needed since some variable name contains extra space(s)
                    temp_col = re.sub(
                        r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", name.strip(), col)
                    new_cols.append(temp_col)
                    continue

            # If not match, just use the original column name
            new_cols.append(col)
        temp_data_norm.columns = new_cols

        temp_df.reset_index(inplace=True, drop=True)
        temp_data_norm.reset_index(inplace=True, drop=True)
        temp_df = pd.concat([temp_df, temp_data_norm], axis=1)

        # Dropping unused columns and rename it
        temp_df.drop(["document_type", "question_answer",
                     "institution_id", "user"], axis=1, inplace=True)
        temp_df.rename(columns={"participant_id": "id_subject",
                       "institution_name": "biobank_nama"}, inplace=True)

        form_group_df_dict[form_group] = temp_df

    # Dump into mysql
    for key in form_group_df_dict.keys():
        temp_df = form_group_df_dict[key].drop(
            ["form_group", "created_by"], axis=1)

        # excluding registry columns which come from registry form
        excl_cols = ["Catatan", "Hub", "Institusi", "Judul Penelitian", "Nomor EC",
                     "PIC (Nama Peneliti)", "Proyek", "Tanggal Kadaluarsa", "Tanggal Dibuat"]

        if not re.findall(r"(?i)rekrutmen_registrasi", key):
            temp_df = form_group_df_dict[key].drop(excl_cols, axis=1)

        temp_df = pd.melt(temp_df,
                          id_vars=["id_subject", "biobank_nama", "version", "created_at"], var_name="path", value_name="value")

        # Set index for the DB
        temp_df.set_index(["id_subject", "biobank_nama",
                          "created_at"], inplace=True)
        # Naming purpose: MySQL has a limit of 64 characters for table name
        key = key.lower()
        key = re.sub(r"(?i)formulir-", "", key)
        key = re.sub(r"(?i)spesifik-penyakit-", "", key)
        key = re.sub(r"--", "-", key)
        key = re.sub(r"examination", "exam", key)

        table_name = f"phenovar_docs_{key}"
        if verbose:
            print(f"Dumping {table_name} to dB")
        with engine.begin() as conn:
            temp_df.to_sql(
                name=f"{table_name}",
                if_exists="replace",
                con=conn,
                dtype={'id_subject': VARCHAR(10), 'biobank_nama': VARCHAR(
                    255), 'created_at': DATETIME}
            )
        if verbose:
            print(f"{table_name} has been dumped")
