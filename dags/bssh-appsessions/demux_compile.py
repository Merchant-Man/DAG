import boto3
import pandas as pd
from io import StringIO

# ---- CONFIG ----
BUCKET_NAME = "your-bucket-name"       # <-- change this
PREFIX = "bssh/Demux/"                 # where all the files are
FILENAME_SUFFIX = "Demultiplex_Stats.csv"  # match this ending

# ---- INIT S3 CLIENT ----
s3 = boto3.client("s3")

# ---- LIST OBJECTS ----
paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)

csv_keys = []

for page in pages:
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(FILENAME_SUFFIX):
            csv_keys.append(key)

print(f"Found {len(csv_keys)} matching CSV files.")

# ---- READ & COMBINE ----
dfs = []

for key in csv_keys:
    print(f"Reading: {key}")
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    content = response["Body"].read().decode("utf-8")

    df = pd.read_csv(StringIO(content))
    df["SourceFile"] = key  # Optional: track origin
    dfs.append(df)

# ---- COMBINE INTO SINGLE DATAFRAME ----
if dfs:
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"✅ Combined DataFrame shape: {combined_df.shape}")
    print(combined_df.head())
else:
    print("❌ No files were read.")
