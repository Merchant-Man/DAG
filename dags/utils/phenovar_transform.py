from typing import Callable, Optional, Dict, Any, List, Union
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from .utils import dict_csv_buf_transform, default_retry_args, extract_db_url
import json
import pandas as pd
import mysql.connector as sql
import time

def transform_demography_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    df['gender'].replace({'female': 'FEMALE', 'male': 'MALE'}, inplace=True)
    df.rename(columns={'id': 'id_subject', 'created_at': 'creation_date',
                       'updated_at': 'updation_date', 'gender': 'sex', 'nik': 'encrypt_nik', 'full_name': 'encrypt_full_name', 'dob': 'encrypt_birth_date'}, inplace=True)

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    df = df[["id_subject", "encrypt_full_name", "encrypt_nik", "encrypt_birth_date", "sex", "source",
             "province", "district", "created_at", "updated_at", "creation_date", "updation_date"]]

    # Even we remove duplicates, API might contain duplicate records for an id_subject
    # So, we will keep the latest record
    df['updation_date'] = pd.to_datetime(df['updation_date'])
    df = df.sort_values('updation_date').groupby('id_subject').tail(1)

    df['updation_date'] = df["updation_date"].dt.strftime(
        '%Y-%m-%d %H:%M:%S').astype('str')

    # Need to fillna so that the mysql connector can insert the data.
    values = {
        "id_subject": "", "encrypt_full_name": "", "encrypt_nik": "", "encrypt_birth_date": "", "sex": "", "source": "", "province": "", "district": "", "creation_date": "", "updation_date": ""
    }
    df.fillna(value=values, inplace=True)
    return df

def transform_variable_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()
    df = df.astype(str)
    return df


def fetch_documents_all_participants_and_dump(api_conn_id: str, db_secret_url: str, data_end_point: str, aws_conn_id: str, bucket_name: str,
                   object_path: str, headers: Optional[Dict[str, Any]] = {},
                   jwt_end_point: str = "", jwt_headers: Optional[Dict[str, Any]] = {},
                   data_payload: Optional[Dict[str, Any]] = {},
                   jwt_payload: Optional[Dict[str, Any]] = {},
                   get_token_function: Optional[Callable[[
                       Dict[str, Any]], str]] = None,
                   retry_args: Dict[str, Any] = default_retry_args,
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

    user, passwd, host, port, db=extract_db_url(db_secret_url)
    conn=sql.connect(db=db, user=user, host=host,
                       password=passwd, use_unicode=True)
    
    id_repos = pd.read_sql_query("SELECT id_subject FROM phenovar_participants", conn)["id_subject"].values
    conn.close()
    
    for idx in range(len(id_repos)):
        if idx%500 == 0:
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
                    access_token = get_token_function(response, response_header)
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
        time.sleep(2) # To prevent rate limiting :(((( please create a bulk request or better filter the last updated document via API. GOSSSHHH

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
