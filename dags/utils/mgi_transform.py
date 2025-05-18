import pandas as pd 

def transform_ztron_pro_samples_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.iloc[: , 1:]
    df = df.drop_duplicates()
    
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df

def transform_ztron_pro_qc_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    df = df.drop_duplicates()
    
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df

def transform_ztron_pro_analysis_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    df = df.drop_duplicates()

    df["date_secondary"] = pd.to_datetime(df["date_secondary"], format="%d-%m-%Y")
    
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df

def transform_analysis_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()
    
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df

def ztron_transform_analysis_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()
    
    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts
    
    df["data_creation_date"] = pd.to_datetime(df["data_creation_date"], format="%d-%m-%Y")

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df

def transform_qc_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()
    
    df['id_repository'] = df['Sample'].str.split('|').apply(lambda x: x[-1].strip()) 
    df['run_name'] = df['Sample'].str.split('|').apply(lambda x: x[-2].strip()) 

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts
    
    cols = {
        'id_repository': 'id_repository',
        'fastqc_raw-percent_duplicates': 'percent_dups',
        'fastqc_raw-percent_gc': 'percent_gc',
        'fastqc_raw-total_sequences': 'total_seqs',
        'samtools_flagstat_stats-non_primary_alignments': 'non_primary',
        'samtools_flagstat_stats-reads_mapped_percent': 'percent_mapped',
        'samtools_flagstat_stats-reads_properly_paired_percent': 'percent_proper_pairs',
        'samtools_flagstat_stats-reads_mapped': 'reads_mapped',
        'mosdepth-50_x_pc': 'at_least_50x',
        'mosdepth-20_x_pc': 'at_least_20x',
        'mosdepth-10_x_pc': 'at_least_10x',
        'mosdepth-median_coverage': 'median_coverage',
        'mosdepth-mean_coverage': 'depth',
        'bcftools_stats-number_of_records': 'vars',
        'bcftools_stats-number_of_SNPs': 'snp',
        'bcftools_stats-number_of_indels': 'indel',
        'bcftools_stats-tstv': 'ts_tv',
        'peddy-predicted_sex_sex_check': 'ploidy_estimation',
        "created_at": "created_at",
        "updated_at": "updated_at",
        "run_name":"run_name"
    }

    df.rename(columns=cols, inplace=True)
    df = df[list(cols.values())]
    df = df[~df['id_repository'].str.contains(r' R[12]$', regex=True)]

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df
