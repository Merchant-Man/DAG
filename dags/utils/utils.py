from typing import Callable, Optional, Dict, Any, Tuple
from airflow.hooks.http_hook import HttpHook
import tenacity
from requests.exceptions import ConnectionError, HTTPError
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import pandas as pd
from io import StringIO, BytesIO
import time
import random
import numpy as np
import mysql.connector as sql
import re


default_retry_args = dict(
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=64),
    stop=tenacity.stop_after_attempt(5),
    retry=ConnectionError
    )

def extract_db_url(db_secret:str) -> Tuple[str, str, str, str]:
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

def dict_csv_buf_transform(data:pd.DataFrame) -> str:
    """
    Transfrom the dictionary data to CSV buffer and return as string of csv.

    Parameters
    ----------
    data : dict
        The data to be transformed

    Retruns
    ----------
    str
        The CSV string
    """ 
    data = pd.DataFrame(data)

    csv_buffer = StringIO()
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

def fetch_jwt_and_dump(api_conn_id:str, data_end_point:str, jwt_end_point:str, aws_conn_id:str, bucket_name:str, object_path:str, data_payload:Optional[Dict[str, Any]] = {}, jwt_payload:Optional[Dict[str, Any]] = {}, is_using_cookie:Optional[bool] = False, response_key_data:str = "", transform_func:Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = dict_csv_buf_transform, max_retries=5, retry_delay=3,**kwargs) -> bool:
    """
    Fetch daily data from JWT API (POST to get the token and GET the data) and dump it to S3. User can also transform the data by passing a function. The function should transform the dictionary input and return it in the dict form.
    
    Parameters
    ----------
    conn_id : str
        The connection id to be used to connect to the API
    data_end_point : str
        The endpoint to get the data
    jwt_end_point : str
        The endpoint to get the JWT token
    data_payload : Dict[str, Any]
        The payload to be sent to the data endpoint
    jwt_payload : Dict[str, Any]
        The payload to be sent to the JWT endpoint
    transform_func : Callable[dict, dict]
        The function to transform the data. Expected to have a function with have dict as input and output as csv buffer
    aws_conn_id : str
        The connection id to be used to connect to the S3 bucket
    
    Returns
    -------
    bool
        True if there is any data, False otherwise
    """
    http_hook = HttpHook(method="POST", http_conn_id=api_conn_id)

    # Using Airflowhook retrying mechanism
    # response = http_hook.run_with_advanced_retry(
    #     endpoint=jwt_end_point,
    #     headers={"Content-Type": "application/json"},
    #     data=json.dumps(jwt_payload),
    #     _retry_args=default_retry_args
    # )
    for i in range(max_retries):
        try:
            response = http_hook.run(
                endpoint=jwt_end_point,
                data=json.dumps(jwt_payload),
                headers={"Content-Type": "application/json"}
            )
        except HTTPError as e:
            if 500 <= e.status_code < 600:
                delay = retry_delay + random.random()
                print(f"Retrying in {delay}")
                time.sleep(delay)
                retry_delay *= 2
            
            if i==max_retries-1:
                raise Exception(f"Failed to retrieve JWT token. Response: {response.text}")

    try:
        access_token = response.json().get("data", {}).get("access_token")
    except JSONDecodeError as e:
        print(f"Decoding into JSON failed: {e}")
        print(f"Failed response: {response}")
        raise ValueError("Failed to decode the response")

    if not access_token:
        raise ValueError("Access token is empty!")


    http_hook = HttpHook(method="GET", http_conn_id=api_conn_id)
    headers = {"Authorization": f"Bearer {access_token}"}
    if is_using_cookie:
        cookie = response.headers.get('Set-Cookie')
        headers["Cookie"] = f"{cookie}"
    
    # Load data
    for i in range(max_retries):
        try:
            response = http_hook.run(
                endpoint=data_end_point,
                headers=headers
            )
        except HTTPError as e:
            if 500 <= e.status_code < 600:
                delay = retry_delay + random.random()
                print(f"Retrying in {delay}")
                time.sleep(delay)
                retry_delay *= 2
            if i==max_retries-1:
                raise Exception(f"Failed to retrieve Data. Response: {e.text}")

    try:
        # no data key
        if "data" not in response.json():
            return False

        data = response.json()["data"]
        if response_key_data!="":
            if response_key_data not in data:
                return False
            data = response.json()["data"][response_key_data]
        
        # Data might be empty - i.e. No update in the data.
        if not data:
            print("No data to update")
            return False
    except JSONDecodeError as e:
        print(f"Decoding into JSON failed: {e}")
        print(f"Failed response: {response}")
        raise ValueError("Failed to decode the response")
    
    if transform_func:
        data = transform_func(data)

    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"
    
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data = data,
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )

    return True

def fetch_and_dump(api_conn_id:str, data_end_point:str,  aws_conn_id:str, bucket_name:str, object_path:str, headers:Optional[Dict[str, Any]] = {},  data_payload:Optional[Dict[str, Any]] = {}, retry_args:[Dict[str, Any]] = {}, response_key_data:str = "", transform_func:Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = dict_csv_buf_transform,**kwargs) -> bool: 
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
    data_payload : Dict[str, Any]
        The payload to be sent to the data endpoint
    transform_func : Callable[dict, dict]
        The function to transform the data. Expected to have a function with have dict as input and output as csv buffer
    
    Returns
    -------
    bool
        True if there is any data, False otherwise
    """

    http_hook = HttpHook(method="GET", http_conn_id=api_conn_id)

    # Using Airflowhook retrying mechanism
    response = http_hook.run_with_advanced_retry(
        endpoint=data_end_point,
        headers=headers,
        _retry_args=retry_args
    )

    try:
        if "data" not in response.json():
            return False
        
        data = response.json()["data"]
        if response_key_data!="":
            if response_key_data not in data:
                return False
            data = response.json()["data"][response_key_data]
    except JSONDecodeError as e:
        print(f"Decoding into JSON failed: {e}")
        print(f"Failed response: {response}")
        raise ValueError("Failed to decode the response")

    if not data:
        print("No data to update")
        return False
    
    if transform_func:
        data = transform_func(data)

    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"
    
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data = data,
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )

    return True


def silver_transform_to_db(aws_conn_id:str, bucket_name:str, object_path:str, transform_func:Callable[[pd.DataFrame], pd.DataFrame], db_secret_url:str, insert_query:str, **kwargs) -> None: 
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
    insert_query : str
        The query to insert the data. Should handling the multiple insertions.
    """
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"

    # Check whether the file is present (means the data is updated)
    if not s3.check_for_key(file_name, bucket_name):
        Print(f"=== Can't find {filename} ===")
        return 

    csv_obj = s3.get_key(key=file_name, bucket_name=bucket_name)
    df = pd.read_csv(BytesIO(csv_obj.get()['Body'].read()))

    df = transform_func(df, kwargs['curr_ds'])

    user, passwd, host, port, db = extract_db_url(db_secret_url)

    conn = sql.connect(db=db,user=user,host=host,password=passwd,use_unicode=True)
    cur = conn.cursor()
    print("====START INSERTING====")
    print(f"Data Points: {df.shape[0]}")
    print(f"QUERY:\n{insert_query}")
    chunk_size = 1000
    for start in range(0, df.shape[0], chunk_size):
        chunk = df[start:start + chunk_size]
        print(f"Chunk data Points: {chunk.shape[0]}")

        print(f"Chunk data:\n{chunk.head()}")
        # print(f"Chunk data:\n{chunk.tail()}")
        data = list(chunk.itertuples(index=False, name=None))
        try:
            # print(data)
            cur.executemany(insert_query,data)
            conn.commit()

        except (sql.Error,sql.Warning) as e:
            conn.close()
            raise ValueError(e)

    print("====FINISHED====")
    