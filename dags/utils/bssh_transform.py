from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from typing import Union


def fetch_and_dump(api_conn_id, data_end_point, aws_conn_id, bucket_name,
                   object_path, headers, data_payload, transform_func, curr_ds, limit: Union[int, None] = 25, **kwargs):

    http_hook = HttpHook(method="GET", http_conn_id=api_conn_id)
    offset = 0
    all_samples = []

    while True:
        list_endpoint = f"{data_end_point}?offset={offset}&limit={limit}&sortDir=Desc&sortBy=DateCreated"
        response = http_hook.run_with_advanced_retry(
            endpoint=list_endpoint,
            headers=headers,
            data=data_payload
        )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch biosamples: {response.status_code}, {response.text}")

        biosamples = response.json().get("Items", [])
        if not biosamples:
            break

        for sample in biosamples:
            biosample_id = sample.get("Id")
            if not biosample_id:
                continue

            detail_endpoint = f"{data_end_point}/{biosample_id}?include=Yields"
            yield_response = http_hook.run_with_advanced_retry(
                endpoint=detail_endpoint,
                headers=headers,
                data=data_payload
            )

            if yield_response.status_code != 200:
                print(f"Failed to fetch yields for biosample {biosample_id}")
                continue

            yield_data = yield_response.json()
            all_samples.append(yield_data)

        offset += limit

    # Convert to DataFrame and transform
    df = pd.DataFrame(all_samples)
    df = transform_func(df, curr_ds)

    file_name = f"{object_path}/{curr_ds}.csv"

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=df.to_csv(index=False),
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )

    return True
