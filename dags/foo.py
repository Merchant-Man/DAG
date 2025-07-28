fetch_and_dump_configs = {
    "demography": {
        "method_request": "GET",  # Use default GET since not specified
        "extra_kwargs": {}
    },
    "category": {
        "method_request": "GET",
        "extra_kwargs": {}
    },
    "variable": {
        "method_request": "GET",
        "extra_kwargs": {}
    },
    "data_sharing": {
        "method_request": "GET",
        "extra_kwargs": {}
    },
    "digital_consent": {
        "method_request": "POST",
        "extra_kwargs": {
            "data_payload": {"date": "{{ ds }}", "per_page": 50, "page": 1},
            "pagination_function": "phenovar_api_paginate",
            "cursor_token_param": "page",
            "headers": {"Content-Type": "application/json"}
        }
    }
}


# Common operator kwargs for fetch_and_dump
common_fetch_kwargs = {
    "jwt_end_point": "JWT_END_POINT",
    "aws_conn_id": "AWS_CONN_ID",
    "jwt_payload": "JWT_PAYLOAD",
    "jwt_headers": {"Content-Type": "application/json"},
    "get_token_function": "get_token_function",
    "response_key_data": "data",
    "curr_ds": "{{ ds }}",
    "prev_ds": "{{ prev_ds }}"
}

op_kwargs = {
    "api_conn_id": "PHENOVAR_CONN_ID",
    "data_end_point": "data_end_point",
    "bucket_name": "S3_DWH_BRONZE",
    "object_path": "object_path",
}
op_kwargs.update(common_fetch_kwargs)
op_kwargs.update(fetch_and_dump_configs.get(
    "digital_consent", {}).get("extra_kwargs", {}))

op_kwargs["method_request"] = fetch_and_dump_configs.get(
     "digital_consent", {}).get("method_request", "GET")
import pprint
pprint.pprint(op_kwargs)

print("method_request" in fetch_and_dump_configs["digital_consent"])