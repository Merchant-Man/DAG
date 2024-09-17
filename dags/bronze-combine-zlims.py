import os
import pandas as pd
import boto3
from datetime import datetime
import re

S3_DWH_BRONZE = 'bgsi-data-dwh-bronze'
LOCAL_TMP_DIR = './tmp'
LOCAL_OUTPUT_DIR = './output'

# NEED TO CHANGE FOR AIRFLOW, NOT LOCAL
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

s3_client = boto3.client('s3')

def get_most_recent_file(prefix, file_pattern):
    print(f"Fetching the most recent file with prefix: {prefix} and pattern: {file_pattern}")
    response = s3_client.list_objects_v2(Bucket=S3_DWH_BRONZE, Prefix=prefix)
    
    matching_files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.csv'):
                if file_pattern == 'YYYYMMDD':
                    if re.match(r'\d{8}\.csv$', os.path.basename(key)):
                        matching_files.append(key)
                elif file_pattern in key:
                    matching_files.append(key)
    
    if not matching_files:
        print(f"No files found matching pattern: {file_pattern}")
        return None
    

    if file_pattern == 'YYYYMMDD':
        matching_files.sort(key=lambda x: os.path.basename(x).split('.')[0], reverse=True)
    else:
        matching_files.sort(key=lambda x: x.split(file_pattern)[1].split('.csv')[0], reverse=True)
    
    # Get the most recent file
    most_recent = matching_files[0]
    local_path = os.path.join(LOCAL_TMP_DIR, most_recent.replace('/', '_'))
    s3_client.download_file(S3_DWH_BRONZE, most_recent, local_path)
    print(f"Downloaded most recent file: {most_recent} to {local_path}")
    return local_path

def clean_data(df):
    # Remove decimal points from Index and Sample Name columns
    df['Index'] = df['Index'].astype(str).str.replace('.0', '', regex=False)
    df['Sample Name'] = df['Sample Name'].astype(str).str.replace('.0', '', regex=False)
    
    # Remove text before dash in Sample Name if it exists and is not empty
    df['Sample Name'] = df['Sample Name'].apply(lambda x: x.split('-')[-1] if pd.notna(x) and '-' in str(x) else x)
    
    return df

def transform_bronze_samples_zlims():
    print("Transforming ZLIMS samples data")
    samples_file = get_most_recent_file('samples/zlims/', 'YYYYMMDD')
    
    if not samples_file:
        print("No samples file found.")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(samples_file)
    except UnicodeDecodeError:
        df = pd.read_csv(samples_file, encoding='ISO-8859-1')
    
    print(f"Columns in samples file: {df.columns.tolist()}")
    
    required_columns = ['Sample ID(*)', 'Flowcell ID', 'Create Time', 'Index(*)', 'Sample Name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: Missing required columns in samples file: {missing_columns}")
        return pd.DataFrame()
    
    transformed_df = pd.DataFrame({
        'id_sample': df['Sample ID(*)'],
        'id_flowcell': df['Flowcell ID'],
        'create_by': 'zlims',
        'create_org': 'bgsi',
        'samples_create_date': df['Create Time'],
        'samples_modification_date': df['Create Time'],
        'species': 'Homo sapiens',
        'library_strategy': 'WGS',
        'platform': 'MGI',
        'instrument_model': 'DNBSEQ-T7',
        'Index': df['Index(*)'],
        'Sample Name': df['Sample Name'],
        'sequencing': 'True',
        'transfer_data': 'True',
        'analysis_primary': 'True'
    })
    
    transformed_df = clean_data(transformed_df)
    
    print(f"Columns in transformed samples DataFrame: {transformed_df.columns.tolist()}")
    return transformed_df

def transform_bronze_qc_zlims():
    print("Transforming QC data")
    qc_file = get_most_recent_file('qc/zlims/', 'mgi_')
    
    if qc_file:
        try:
            df = pd.read_csv(qc_file)
            print(f"Columns in QC file: {df.columns.tolist()}")
            if 'id_sample' not in df.columns:
                print("Error: 'id_sample' column not found in QC file")
                return pd.DataFrame()
            return df
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {qc_file}")
        except Exception as e:
            print(f"Error reading file {qc_file}: {str(e)}")
    
    print("No valid QC data file found.")
    return pd.DataFrame()

def combine_and_quarantine_zlims_data(samples_df, qc_df):
    print("Combining ZLIMS data and applying quarantine logic")
    
    if samples_df.empty:
        print("Error: Samples DataFrame is empty")
        return None, None, None
    
    if qc_df.empty:
        print("Error: QC DataFrame is empty")
        return None, None, None
    
    print(f"Columns in samples DataFrame: {samples_df.columns.tolist()}")
    print(f"Columns in QC DataFrame: {qc_df.columns.tolist()}")
    
    # MERGE WITH QC
    df_combined = samples_df.merge(qc_df, on='id_sample', how='left')
    
    # Apply quarantine logic
    if 'percent_dups' not in df_combined.columns:
        print("Error: 'percent_dups' column not found in combined DataFrame")
        return None, None, None
    
    # Identify bad data
    bad_data = df_combined[
        (df_combined['id_flowcell'].isna()) | 
        (df_combined['Index'].isna()) | 
        (df_combined['Index'].astype(str).str.len() > 200) |
        (df_combined['id_sample'].str.contains('Test|test|_|-', case=False, na=False)) |
        (df_combined['Index'].astype(str).str.len() > 3) |
        (df_combined['id_sample'].str.contains(r'[A-Za-z]{4,}', regex=True))
    ]
    
    # Remove bad data from combined DataFrame
    df_combined = df_combined[~df_combined.index.isin(bad_data.index)]
    
    # quarantine
    clean_df = df_combined.dropna(subset=['percent_dups'])
    quarantine_df = df_combined[df_combined['percent_dups'].isna()]
    
    # Move rows from quarantine to clean if Index and Sample Name are the same
    move_to_clean = quarantine_df[quarantine_df['Index'].astype(str) == quarantine_df['Sample Name'].astype(str)]
    clean_df = pd.concat([clean_df, move_to_clean])
    quarantine_df = quarantine_df[~quarantine_df.index.isin(move_to_clean.index)]
    
    # Save clean data
    clean_output_path = os.path.join(LOCAL_OUTPUT_DIR, 'zlims-combined-clean.csv')
    clean_df.to_csv(clean_output_path, index=False)
    print(f"Clean combined data saved to: {clean_output_path}")
    
    # Save quarantined data
    quarantine_output_path = os.path.join(LOCAL_OUTPUT_DIR, 'zlims-combined-quarantine.csv')
    quarantine_df.to_csv(quarantine_output_path, index=False)
    print(f"Quarantined data saved to: {quarantine_output_path}")
    
    # Save bad data
    bad_data_output_path = os.path.join(LOCAL_OUTPUT_DIR, 'bad_data.csv')
    bad_data.to_csv(bad_data_output_path, index=False)
    print(f"Bad data saved to: {bad_data_output_path}")
    
    return clean_output_path, quarantine_output_path, bad_data_output_path

def main():
    print("Starting ZLIMS data processing")
    
    # Transform samples
    samples_df = transform_bronze_samples_zlims()
    
    # Transform QC data
    qc_df = transform_bronze_qc_zlims()
    
    # Combine data and apply new quarantine logic
    clean_path, quarantine_path, bad_data_path = combine_and_quarantine_zlims_data(samples_df, qc_df)
    
    if clean_path and quarantine_path and bad_data_path:
        print(f"ZLIMS data processing completed. Results saved in {LOCAL_OUTPUT_DIR}")
        print(f"Clean combined data: {clean_path}")
        print(f"Quarantined data: {quarantine_path}")
        print(f"Bad data: {bad_data_path}")
    else:
        print("ZLIMS data processing failed. Check the error messages above.")

if __name__ == "__main__":
    main()