from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from datetime import datetime
from boto3.dynamodb.conditions import Attr
import pandas as pd
from io import StringIO
import logging 

def fetch_dynamodb_and_load_to_s3(aws_conn_id: str, dynamodb_table: str, bronze_bucket: str, bronze_object_path: str, **kwargs):
    try:
        # Fetch data from DynamoDB
        table = DynamoDBHook(aws_conn_id=aws_conn_id, resource_type='dynamodb').get_conn().Table(dynamodb_table)

        ds = kwargs.get("ds", "2025-03-02")
        # ds="2025-01-01"

        ds_datetime = datetime.strptime(ds, "%Y-%m-%d")
        ts = ds_datetime.strftime('%Y:%m:%d %H:%M:%S') 
        print(f"[DEBUG] Using timestamp filter: {ts}")
        response = table.scan(
            FilterExpression=Attr('time_requested').exists()
        )
        items = response.get('Items', [])
        print(f"[DEBUG] Found {len(items)} items with 'time_requested' field")
        for item in items:
            print("[DEBUG] time_requested:", item.get("time_requested"))
        items = table.scan(Limit=5).get("Items", [])
        for i, item in enumerate(items):
            print(f"[DEBUG] Item {i}: {item}")
        # Filter items with timestamps >= this day
        response = table.scan(
            FilterExpression=Attr('time_requested').gte(ts)
        )
        data_items = response.get('Items', [])

        if not data_items:
            logging.info("[INFO] No data found for the given timestamp. Skipping further steps.")
            return  # Skip the task if no data is found

        # Transform data into a DataFrame
        df = pd.DataFrame(data_items)
        if "created_at" not in df.columns:
            df["created_at"] = ts
        if "updated_at" not in df.columns:
            df["updated_at"] = ts
            
        # Split by 'fix_type' and upload to S3
        fix_types = ['id_repository'
                     , 'id_library'
                     , 'cram_path']
        s3 = S3Hook(aws_conn_id=aws_conn_id)

        for fix_type in fix_types:
            # Filter the dataframe for each fix_type
            group = df[df['fix_type'] == fix_type]

            if group.empty:
                logging.info(f"[INFO] No data for fix_type {fix_type}. Skipping upload.")
                continue

            # Convert to CSV format
            csv_buffer = StringIO()
            group.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()

            # Define S3 key
            s3_key = f'{bronze_object_path}/{fix_type}/{ds}.csv'

            # Upload to S3
            s3.load_string(
                string_data=csv_data,
                key=s3_key,
                bucket_name=bronze_bucket,
                replace=True
            )
            logging.info(f"[INFO] Successfully uploaded {fix_type} data to S3 at {s3_key}.")

    except Exception as e:
        logging.error(f"[ERROR] Failed to process data: {e}")
        raise  

def transform_fix_samples_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    insert_columns=["id_repository"
            ,"id_library"
            ,"sequencer"
            ,"id_requestor"
            ,"created_at"
            ,"updated_at"
            ,"time_requested"
            ,"fix_type"
            ,"new_repository"
            ,"new_library"
            ,"id_zlims_index"
            ,"new_index"]
    # Remove duplicates
    df = df.drop_duplicates(subset=['id_repository','id_library'])
    # Ensure all columns exist
    for col in insert_columns:
        if col not in df.columns:
            df[col] = ""
    # Subset and enforce column order
    df = df[insert_columns]
    # Convert all values to strings and replace NaNs
    df = df.fillna("").astype(str)
    return df

def transform_fix_analysis_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    insert_columns = [
        "id_repository", "sequencer", "run_name", "id_requestor",
        "created_at", "updated_at", "time_requested", "fix_type",
        "new_repository", "cram", "new_cram", "vcf", "new_vcf"
    ]

    df = df.drop_duplicates(subset=["run_name"])
    for col in insert_columns:
        if col not in df.columns:
            df[col] = ""
    df = df[insert_columns]
    df = df.fillna("").astype(str)

    return df
