import pandas as pd


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


def transform_samples_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # df = df.drop_duplicates()
    #drop old date (before 20250321 updates)
    df_test["non_null_count"] = df.notnull().sum(axis=1)
    df_test = df_test.sort_values("non_null_count", ascending=False)
    df_test = df_test.drop_duplicates(subset="bam_folder", keep="first")
    df_test = df_test.drop(columns="non_null_count")

    datetime_cols = ['date_upload', 'started_at', 'acquisition_stopped', 'processing_stopped']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ') # Convert datetime columns to UTC and ISO 8601 format

    # Assuming df is your DataFrame
    grouped_df = df.groupby('id_repository').agg({
        'alias': list,  # Aggregate 'alias' as a list
        'total_passed_bases': list,  # Aggregate 'total_passed_bases' as a list
        'bam_size': list,  # Aggregate 'bam_size' as a list
        'date_upload': list,  # Aggregate 'date_upload' as a list
        'total_bases': list,  # Aggregate 'total_bases' as a list
        'passed_bases_percent': list,  # Aggregate 'passed_bases_percent' as a list
        'bam_folder': list,  # Aggregate 'bam_folder' as a list
        'id_library': list,  # Aggregate 'id_library' as a list
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

    # Create a new column `id_library` where the value is just one item from the list
    grouped_df['id_batch'] = grouped_df['id_library'].apply (lambda x: x[0] if x else None)

    if "created_at" not in df.columns:
        grouped_df["created_at"] = ts
    if "updated_at" not in df.columns:
        grouped_df["updated_at"] = ts

    grouped_df = grouped_df.astype(str)
    grouped_df.fillna(value="", inplace=True)

    grouped_df=grouped_df[["id_repository","alias","total_passed_bases","bam_size","date_upload","total_bases","passed_bases_percent","bam_folder","id_library","sum_of_total_passed_bases","total_bam_size","id_batch","created_at","updated_at","started_at","acquisition_stopped","processing_stopped","instrument","position","id_flowcell"]]
    return grouped_df

