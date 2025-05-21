from typing import Callable, Optional, Dict, Any, Tuple, List, Union
from airflow.providers.http.hooks.http import HttpHook
import tenacity
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
import json
import pandas as pd
import io
import mysql.connector as sql
import re
from airflow.hooks.base import BaseHook
import logging
from pandas.errors import EmptyDataError 

default_retry_args = dict(
    wait=tenacity.wait_exponential(),
    stop=tenacity.stop_after_attempt(10),
    retry=tenacity.retry_if_exception_type(Exception),
)

def get_token_request(api_conn_id:str, jwt_end_point:str, jwt_headers:Optional[Dict[str, Any]] = {}, jwt_payload:Optional[Dict[str, Any]] = {}, get_token_function:Optional[Callable[[Dict[str, Any]], str]] = None, retry_args:Dict[str, Any] = default_retry_args) -> str:
    http_hook = HttpHook(method="POST", http_conn_id=api_conn_id)

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
        headers = get_token_function(response, response_header)
    except json.JSONDecodeError as e:
        print(f"Decoding into JSON failed: {e}")
        print(f"Failed response: {response}")
        raise ValueError("Failed to decode the response")
    return headers

def extract_db_url(db_secret: str) -> Tuple[str, str, str, str]:
    """
    Extract database information from db_secret in the format

    Parameters
    ----------
    db_secret: str
        The database secret in the format <db_con>://<username>:<password>@<host>:<port>/<database_name>

    Returns
    -------
        user_name, passwd, host, port, db_name
    """
    pattern = r"([^:]+)://([^:]+):([^@]+)@([^:]+):(\d+)/(.*)"
    groups = re.search(pattern, db_secret)
    if not groups:
        raise ValueError("Invalid db_secret format")

    user_name = groups.group(2)
    passwd = groups.group(3)
    host = groups.group(4)
    port = groups.group(5)
    db_name = groups.group(6)
    return user_name, passwd, host, port, db_name


def dict_csv_buf_transform(data: Dict[str, Any]) -> str:
    """
    Transfrom the dictionary data to CSV buffer and return as string of csv.

    Parameters
    ----------
    data : Dict[str, Any]
        The data in Dict to be transformed

    Retruns
    ----------
    str
        The CSV string
    """
    data = pd.DataFrame(data)

    csv_buffer = io.StringIO()
    data.to_csv(csv_buffer, index=False)

    return csv_buffer.getvalue()


def s3_csv_to_pd(aws_conn_id, file_name, bucket_name):
    """
    Fetch s3 object and convert into pandas.
    """
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    csv_obj = s3.get_key(
        key=file_name,
        bucket_name=bucket_name
    )
    df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
    return df


def fetch_and_dump(api_conn_id: str, data_end_point: str, aws_conn_id: str, bucket_name: str,
                   object_path: str, headers: Optional[Dict[str, Any]] = {},
                   jwt_end_point: str = "", jwt_headers: Optional[Dict[str, Any]] = {},
                   data_payload: Optional[Dict[str, Any]] = {},
                   jwt_payload: Optional[Dict[str, Any]] = {},
                   get_token_function: Optional[Callable[[
                       Dict[str, Any]], str]] = None,
                   retry_args: Dict[str, Any] = default_retry_args,
                   response_key_data: Union[List[str], str] = "data", transform_func: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = dict_csv_buf_transform,
                   pagination_function: Optional[Callable[[
                       Dict[str, Any]], str]] = None,
                   offset_pagination: Optional[bool] = False, cursor_token_param: str = "pageToken", offset_param: str = "offset",
                   limit_param: str = "limit", limit: Union[int, None] = None, **kwargs) -> bool:
    """
    Fetch daily data from API and dump it to S3. User can also transform the data by passing a function. The function should transform the dictionary input and return it in the dict form.

    Parameters
    ----------
    api_conn_id : str
        The connection id to be used to connect to the API
    data_end_point : str
        The endpoint to get the data
    aws_conn_id : str
        The connection id to be used to connect to the S3 bucket
    bucket_name : str
        S3 bucket name
    object_path : str
        The path to store the object in the bucket
    headers : Dict[str, Any]
        Dictionary that will be sent as headers to the request
    jwt_end_point : str
        The end point to get the JWT token
    jwt_headers : Dict[str, Any]
        Dictionary that will be sent as headers to the JWT to get the auth token
    data_payload : Dict[str, Any]
        The payload to be sent to the data endpoint (parameters)
    jwt_payload : Dict[str, Any]
        The payload to be sent to the JWT to get the auth token
    get_token_function : Callable[Dic[str, any], str]
        Function to parse the token from the JWT response
    retry_args : [Dict[str, Any]]
        Retry arguments that will be fed to the _retry_args of the Airflow http_hook
    response_key_data : str|List[str]
        Key to get the data from the API. It can be either string or list. If list is given, it will iterate through all the elements of the list.
    transform_func : Callable[Dict[str, Any], Dict[str, Any]]
        The function to transform the data. Expected to have a function with have dict as input and output as csv buffer
    pagination_function : Callable[[Dict[str, Any]], str]
        The function for paginating the API. It NEEDS TO ACCEPT requests.Response object and returning the next page token!  to get the next page endpoint from the response.
    offset_pagination : bool
        Whether to use offset pagination
    offset_param : str
        The parameter name for offset in the API request
    limit_param : str
        The parameter name for limit in the API request
    limit : int
        The number of records to fetch per request/page

    Returns
    -------
    bool
        True if there is any data and successfully dumped. False otherwise.
    """
    access_token = {}
    # If need auth, jwt is called
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
    http_hook = HttpHook(method="GET", http_conn_id=api_conn_id)

    # Whether using cursor, offset, or bulk, if the API provide limit parameter.
    if limit and limit_param:
        data_payload[limit_param] = limit

    print("=== Fetching Data ===")
    all_data = []
    offset = 0
    if offset_pagination:
        data_payload[offset_param] = offset
    while True:
        response = http_hook.run_with_advanced_retry(
            endpoint=data_end_point,
            headers=headers,
            # In GET, the data will be directed to the params which shoul be okay to have python Dict. This fixes the bug with ICA token where previously we have json.dumps(data_payload)!
            data=data_payload,
            _retry_args=retry_args
        )
        try:
            response_json = response.json()
            # Since every API has their own response key, we need to handle it!
            # In case it is not in the root level, we need to go deeper.
            if response_key_data and isinstance(response_key_data, str):
                if response_key_data not in response_json:
                    print("Data key not found in the response!")
                    return False
                data = response_json[response_key_data]
            elif response_key_data and isinstance(response_key_data, list):
                for key in response_key_data:
                    # Forcing key to be str for handle non-string key
                    key = str(key)
                    if key in response_json:
                        response_json = response_json[key]
                    else:
                        print(f"Data key {key} not found in the response!")
                        return False
                data = response_json
            else:
                raise ValueError(
                    "Invalid response_key_data! It should be either string or list of string!")

            # Handle the case when the key is found but the value is empty.
            # If yeas, let's just leave the loop.
            if not data:
                if ((not pagination_function) or (pagination_function and (not all_data))):
                    # break if data is not found for non-pagination API OR
                    # paginated API (with pagination function) which not found any data in the first page.
                    print(
                        f"No data! on the response from the {response_key_data}")
                elif (pagination_function and all_data):
                    # No more data to be traversed
                    print(f"There is no more additional data! Ending the loop.")
                break
            all_data.extend(data)
            if pagination_function and (not offset_pagination):
                print("=== PAGINATING ===")
                # Next page should be the change in the param. nextToken for cursor pagination or the increased offset for offset pagination.
                nextPageToken = pagination_function(response_json)
                if not nextPageToken:
                    # If next cursor pagination token not found, terminated
                    break
                data_payload[cursor_token_param] = nextPageToken
                print(f'Next cursor token: {nextPageToken}')
            elif pagination_function and offset_pagination:
                print("=== PAGINATING ===")
                # if cur_data less than limit, then there will be no more data for the nexzt offset pagination page!
                if len(data) < limit:
                    break
                offset += limit
                print(f'Next offset: {offset}')
            else:  # The case where the bulk request occurs!
                break

        except json.JSONDecodeError as e:
            print(f"Decoding into JSON failed: {e}")
            print(f"Failed response: {response}")
            raise ValueError("Failed to decode the response")

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


def dynamo_and_dump(aws_conn_id: str, table_name: str, bucket_name: str,
                   object_path: str, **kwargs) -> None:
    """
    Fetching data from dynamodb and dump it into s3 bucket. Currently only support ALL_SELECTION with scan operation.

    Parameters
    ----------
    aws_conn_id : str
        The connection id to be used to connect to the DynamoDB and the S3 Bronze Bucket
    table_name : str
        Table name to be fetched
    bucket_name : str
        Table name to store the result form dyanmodb
    object_path : str
        The path to store the object inside the bucket

    Returns
    -------
    bool
        True if there is any data and successfully dumped. False otherwise.

    """
    # Get the client from Airflow hook. Note: Airflow has a not so complete hook functionalities.
    dynamodb_client = DynamoDBHook(aws_conn_id=aws_conn_id).get_conn().meta.client
    try:
        resp = dynamodb_client.scan(
            TableName=table_name, Select="ALL_ATTRIBUTES")
        items = []
        if not resp['Items']:
            print("No data found in the table")
            return False

        while True:
            items.extend(resp['Items'])
            if "LastEvaluatedKey" not in resp:
                break
            resp = dynamodb_client.scan(
                TableName=table_name, Select="ALL_ATTRIBUTES", ExclusiveStartKey=resp["LastEvaluatedKey"])

    except Exception as e:
        print(e)
        raise Exception("Failed to fetch data from dynamodb")

    df = pd.json_normalize(items)
    new_col_names = []
    for col in df.columns:
        temp = re.findall(r"([\w\d]+)\.", col)
        new_col_names.append(temp[0] if temp else col)
    df.columns = new_col_names
    print(df.head())

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )

    return True

def silver_transform_to_db(aws_conn_id: str, bucket_name: str, object_path: str, db_secret_url: str, transform_func: Callable[[pd.DataFrame], pd.DataFrame] = None, multi_files: bool=False, all_files=False, **kwargs) -> None:
    """
    Transforming s3 data and insert into db.

    Parameters
    ----------
    aws_conn_id : str
        The connection id to be used to connect to the S3 bucket
    bucket_name : str
        The bucket name
    object_path : str
        The object path of the bucket
    transform_func : Callable[[pd.DataFrame], pd.DataFrame]
        The function to transform the data
    db_secret_url : str
        The database secret url
    multi_files : bool
        Flag to indicates whether to process exact file with '{ds}.csv' or multiple files with '{ds}' in the name.
    """
    s3=S3Hook(aws_conn_id=aws_conn_id)
    curr_ds=kwargs["curr_ds"]

    # Build the prefix ensuring it ends with /
    prefix=object_path if object_path.endswith('/') else object_path + '/'
    file_keys=s3.list_keys(bucket_name=bucket_name, prefix=prefix)
    # print(file_keys)

    if not file_keys:
        logging.warning(
            f"=== No files found in bucket {bucket_name} with prefix {prefix} ===")
        return

    if all_files:  # for the current case of illumina QS
        print(f"All file keys that will be appended: {file_keys}")
        df=pd.DataFrame()
        for file_key in file_keys:
            if file_key.endswith('.csv'):
                # Read each CSV file into a DataFrame
                csv_obj=s3.get_key(bucket_name=bucket_name, key=file_key)
                content = csv_obj.get()['Body'].read()
                try:
                    temp_df = pd.read_csv(io.BytesIO(content))
                    if not temp_df.empty:
                        df = pd.concat([df, temp_df], ignore_index=True)
                    else:
                        continue
                except EmptyDataError:
                    continue


    elif multi_files:
        # Regex pattern matching any file that has curr_ds in its name and ends with .csv
        df=pd.DataFrame()
        pattern=re.compile(rf".*{re.escape(curr_ds)}.*\.csv")
        matched_file_list=[]
        for key in file_keys:
            # print(f"checking {key}")
            if pattern.match(key):
                matched_file_list.append(key)

        if not matched_file_list:
            logging.warning(
                f"=== Can't find any file with {curr_ds} in S3 ===")
            return

        print(f"Matched files: {matched_file_list}")

        for file_name in matched_file_list:
            csv_obj=s3.get_key(key=file_name, bucket_name=bucket_name)
            temp_df=pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
            df=pd.concat([df, temp_df], ignore_index=True)
    else:
        file_name=f"{object_path}/{kwargs['curr_ds']}.csv"
        # Check whether the file is present (means the data is updated to the s3)
        if not s3.check_for_key(file_name, bucket_name):
            logging.warning(f"=== Can't find {file_name} in S3 ===")
            return

        csv_obj=s3.get_key(key=file_name, bucket_name=bucket_name)
        df=pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
        
    if transform_func:
        df=transform_func(df, kwargs['curr_ds'])

    if df.empty:
        logging.warning(f"=== No data to update ===")
        return

    user, passwd, host, port, db=extract_db_url(db_secret_url)

    conn=sql.connect(db=db, user=user, host=host,
                       password=passwd, use_unicode=True)
    cur=conn.cursor()
    insert_query=kwargs["templates_dict"]["insert_query"]
    print("====START INSERTING====")
    print(f"Data Points: {df.shape[0]}")
    print(f"QUERY:\n{insert_query}")
    chunk_size=1000
    for start in range(0, df.shape[0], chunk_size):
        chunk=df[start:start + chunk_size]
        print(f"Chunk data Points: {chunk.shape[0]}")

        print(f"Chunk data:\n{chunk.head()}")
        # print(f"Chunk data:\n{chunk.tail()}")
        data=list(chunk.itertuples(index=False, name=None))
        try:
            # print(data)
            cur.executemany(insert_query, data)
            conn.commit()

        except (sql.Error, sql.Warning) as e:
            conn.close()
            raise ValueError(e)
    print("====FINISHED====")

    if "dedup_query" in kwargs["templates_dict"]:
        dedup_query=kwargs["templates_dict"]["dedup_query"]
        print("====START DEDUPLICATION====")
        try:
            cur.execute(dedup_query)
            conn.commit()
            print("====FINISHED DEDUPLICATION====")
        except (sql.Error, sql.Warning) as e:
            conn.close()
            raise ValueError(e)

    conn.close()
