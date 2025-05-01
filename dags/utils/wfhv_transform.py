import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json

def transform_analysis_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    # Convert date_start to datetime for sorting
    df["date_start"] = pd.to_datetime(df["date_start"])

    # Keep only the latest record for each id_repository
    df = df.sort_values("date_start").drop_duplicates(
        "id_repository", keep="last")

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df


def transform_qc_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    cols = {
        'id_repository': df['id_repository'],
        'run_name': df['run_name'],
        'contaminated': df['Contaminated'],
        'n50': df['N50'],
        'yield': df['Yield'],
        'total_seqs': df['Read number'],
        'percent_mapped': df['Reads mapped (%)'],
        'median_read_quality': df['Median read quality'],
        'median_read_length': df['Median read length'],
        'chromosomal_depth': df['Chromosomal depth (mean)'],
        'total_depth': df['Total depth (mean)'],
        'snv': df['SNVs'],
        'indel': df['Indels'],
        'ts_tv': df['Transition/Transversion rate'],
        'sv_insertion': df['SV insertions'],
        'sv_deletion': df['SV deletions'],
        'sv_others': df['Other SVs'],
        'ploidy_estimation': df['Predicted sex chromosome karyotype'],
        'at_least_1x': df['Bases with >=1-fold coverage'] / 3200000000 * 100,
        'at_least_10x': df['Bases with >=10-fold coverage'] / 3200000000 * 100,
        'at_least_15x': df['Bases with >=15-fold coverage'] / 3200000000 * 100,
        'at_least_20x': df['Bases with >=20-fold coverage'] / 3200000000 * 100,
        'at_least_30x': df['Bases with >=30-fold coverage'] / 3200000000 * 100,
    }
    # Construct the new DataFrame
    df = pd.DataFrame(cols)

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df


def transform_samples_data(df: pd.DataFrame, ts: str, fix_bucket: str, fix_prefix: str, aws_conn_id: str) -> pd.DataFrame:
    df["non_null_count"] = df.notnull().sum(axis=1)
    df = df.sort_values("non_null_count", ascending=False)
    df = df.drop_duplicates(subset="bam_folder", keep="first").drop(columns="non_null_count")

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_client = s3_hook.get_conn()

    response = s3_client.list_objects_v2(Bucket=fix_bucket, Prefix=fix_prefix)
    files = sorted(
        [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')],
        reverse=True
    )

    if files:
        latest_fix_key = files[0]
        print(f"Loading fix file: {latest_fix_key}")
        fix_obj = s3_client.get_object(Bucket=fix_bucket, Key=latest_fix_key)
        fix_df = pd.read_csv(fix_obj['Body'])

        fix_df_ont = fix_df[fix_df['sequencer'] == 'ONT']
        if not fix_df_ont.empty:
            id_mapping = fix_df_ont.set_index('id_repository')['new_repository'].to_dict()
            df['id_repository'] = df['id_repository'].replace(id_mapping)
    else:
        print("No fix files found in S3.")

    datetime_cols = ['date_upload', 'started_at', 'acquisition_stopped', 'processing_stopped']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    grouped_df = df.groupby('id_repository').agg({
        'alias': list,
        'total_passed_bases': list,
        'bam_size': list,
        'date_upload': list,
        'total_bases': list,
        'passed_bases_percent': list,
        'bam_folder': list,
        'id_library': list,
        'started_at': list,
        'acquisition_stopped': list,
        'processing_stopped': list,
        'instrument': list,
        'position': list,
        'id_flowcell': list
    }).reset_index()

    grouped_df['total_bam_size'] = grouped_df['bam_size'].apply(
        lambda x: sum(filter(None, [int(i) for i in x if pd.notnull(i)]))
    ).astype(str)

    grouped_df['sum_of_total_passed_bases'] = grouped_df['total_passed_bases'].apply(
        lambda x: sum(filter(None, [int(i) for i in x if pd.notnull(i)]))
    ).astype(str)

    grouped_df['id_batch'] = grouped_df['id_library'].apply(lambda x: x[0] if x else None)
    grouped_df["created_at"] = ts
    grouped_df["updated_at"] = ts

    list_cols = [
        'alias', 'total_passed_bases', 'bam_size', 'date_upload', 'total_bases',
        'passed_bases_percent', 'bam_folder', 'id_library', 'started_at',
        'acquisition_stopped', 'processing_stopped', 'instrument', 'position', 'id_flowcell'
    ]

    def clean_and_serialize_list(x):
        if isinstance(x, list):
            return json.dumps([i if pd.notnull(i) else "" for i in x])
        return x

    for col in list_cols:
        grouped_df[col] = grouped_df[col].apply(clean_and_serialize_list)

    grouped_df = grouped_df.fillna("").astype(str)

    grouped_df = grouped_df[[
        "id_repository", "alias", "total_passed_bases", "bam_size", "date_upload",
        "total_bases", "passed_bases_percent", "bam_folder", "id_library",
        "sum_of_total_passed_bases", "total_bam_size", "id_batch", "created_at",
        "updated_at", "started_at", "acquisition_stopped", "processing_stopped",
        "instrument", "position", "id_flowcell"
    ]]

    return grouped_df

