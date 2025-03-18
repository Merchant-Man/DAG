import pandas as pd
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
    df["id_subject"] = df["ehr_id"]
    df["sex"] = df["gender"].replace(
        {"Perempuan": "FEMALE", "Laki-laki": "MALE"})
    df["age"] = df["age"].astype("int64")

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df.fillna(value="", inplace=True)

    df = df[["id_subject", "age", "sex",
             "created_at", "updated_at", "creationDate"]]

    # Even we remove duplicates, RegINA API might contain duplicate records for an id_subject
    # So, we will keep the latest record
    df['creationDate'] = pd.to_datetime(df['creationDate'])
    df = df.sort_values('creationDate').groupby('id_subject').tail(1)

    return df


def fetch_documents_all_participants_and_dump(
    api_conn_id: str,
    db_secret_url: str,
    data_end_point: str,
    aws_conn_id: str,
    bucket_name: str,
    object_path: str,
    get_id_query: str,
    headers: Optional[Dict[str, Any]] = {},
    jwt_end_point: str = "",
    jwt_headers: Optional[Dict[str, Any]] = {},
    data_payload: Optional[Dict[str, Any]] = {},
    jwt_payload: Optional[Dict[str, Any]] = {},
    get_token_function: Optional[Callable[[Dict[str, Any]], str]] = None,
    retry_args: Dict[str, Any] = default_retry_args,
    response_key_data: Union[List[str], str] = "data",
    transform_func: Optional[Callable[[Dict[str, Any]],
                                      Dict[str, Any]]] = dict_csv_buf_transform,
    **kwargs
) -> bool:
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
    id_repos = pd.read_sql_query(get_id_query, conn)["id_subject"].values
    conn.close()

    chunk = 500

    def _get_jwt_token() -> Dict[str, Any]:
        if jwt_end_point and get_token_function and (jwt_headers or jwt_payload):
            print("=== Fetching JWT token ===")
            http_hook = HttpHook(method="POST", http_conn_id=api_conn_id)
            # print(f"===\nURI: {BaseHook.get_connection(api_conn_id).get_uri()}\nEnd Point: {jwt_end_point}\nJWT payload: {jwt_payload}\nJWT headers: {jwt_headers}\n===")

            response = http_hook.run_with_advanced_retry(
                endpoint=jwt_end_point,
                headers=jwt_headers,
                # In POST method, the http_hook will pass to the BODY of the request. Therefore it should be JSON stringified!
                data=json.dumps(jwt_payload),
                _retry_args=retry_args
            )
            try:
                response_header = response.headers
                response = response.json()
                # The get_token_function should return a dictionary containing either "headers" or "params" key. i.e. {"headers": {"Authorization": "<token>"}}
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
        return {}

    def _dump_data(data: Any, batch_number: int) -> None:
        if transform_func:
            data = transform_func(data)
        if 'curr_ds' not in kwargs:
            raise KeyError("The key 'curr_ds' is missing in kwargs")
        file_name = f"{object_path}/{kwargs['curr_ds']}_{batch_number}.csv"
        s3 = S3Hook(aws_conn_id=aws_conn_id)
        s3.load_string(
            string_data=data,
            key=file_name,
            bucket_name=bucket_name,
            replace=True
        )

    all_data = []
    query_temp =  data_payload["query"]
    # Loop over the list of id_subject values.
    for idx, id_subject in enumerate(id_repos):
        # Every 'chunk' iterations, refresh the JWT token and dump the current batch.
        if idx % chunk == 0:
            if all_data:
                batch_number = (idx // chunk)
                _dump_data(all_data, batch_number)
                all_data = []
            token_info = _get_jwt_token()
            if token_info:
                if "headers" in token_info:
                    headers.update(token_info["headers"])
                elif "params" in token_info:
                    data_payload.update(token_info["params"])
                else:
                    raise ValueError(
                        "get_token_function doesn't return 'headers' or 'params'! which is required to be integrated for subsequence response to get the data."
                    )
            http_hook = HttpHook(method="POST", http_conn_id=api_conn_id)
        data_payload["query"] = query_temp.format(
            id_repository=id_subject)
        # print(f"===\nURI: {BaseHook.get_connection(api_conn_id).get_uri()}\nEnd Point: {jwt_end_point}\nJWT payload: {jwt_payload}\nJWT headers: {jwt_headers}\n===")
        # print(f"data end point: {data_end_point}, headers: {headers}, data: {json.dumps(data_payload)}")
        response = http_hook.run_with_advanced_retry(
            endpoint=data_end_point,
            headers=headers,
            data=json.dumps(data_payload),
            _retry_args=retry_args
        )
        try:
            response_json = response.json()

            if response_key_data and isinstance(response_key_data, str):
                if response_key_data not in response_json:
                    continue
                data = response_json.get(response_key_data)
            elif response_key_data and isinstance(response_key_data, list):
                for key in response_key_data:
                    key = str(key)
                    if key in response_json:
                        response_json = response_json[key]
                    else:
                        print(f"Data key {key} not found in the response!")
                        continue
                data = response_json
            if not data:
                continue
            all_data.extend(data)
        except json.JSONDecodeError as e:
            print(f"Decoding into JSON failed: {e}")
            print(f"Failed response: {response}")
            continue
        time.sleep(1)  # To prevent rate limiting

    if not all_data:
        print("No data to update")
        return False

    batch_number = ((len(id_repos) - 1) // chunk) + 1
    _dump_data(all_data, batch_number)
    return True


def transform_document_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    df.columns = ["id_subject", "composition_id", "composition_name", "entry"]
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    df["entry"] = df["entry"].apply(json.dumps)
    return df
