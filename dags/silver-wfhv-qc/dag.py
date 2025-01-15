from datetime import datetime, timedelta
import pandas as pd
import io
import ast

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER=Variable.get("S3_DWH_SILVER")
prefix="wfhv/qc/"

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver-wfhv-qc',
    default_args=default_args,
    description='ETL pipeline to merge CSV files from S3',
    schedule_interval=timedelta(days=1),
)

def fetch_data(**kwargs):
    s3 = S3Hook(aws_conn_id='aws')
    
    # List all objects in the S3 prefix
    files = s3.list_keys(bucket_name=S3_DWH_BRONZE, prefix=prefix)
    
    if not files:
        raise ValueError(f"No files found in {prefix}")
    
    all_data_frames = []

    for file_key in files:
        if file_key.endswith('.csv'):
            # Read each CSV file into a DataFrame
            csv_obj = s3.get_key(key=file_key, bucket_name=S3_DWH_BRONZE)
            df = pd.read_csv(io.BytesIO(csv_obj.get()['Body'].read()))
            all_data_frames.append(df)

    # Merge all DataFrames into one
    merged_df = pd.concat(all_data_frames, ignore_index=True)

    # Convert merged DataFrame to CSV format
    csv_buffer = io.StringIO()
    merged_df.to_csv(csv_buffer, index=False)

    return csv_buffer.getvalue()

def transform_data(merged_data: str, **kwargs):
    # Read the CSV data into a DataFrame
    df = pd.read_csv(io.StringIO(merged_data))

    # Remove duplicates
    df = df.drop_duplicates()

    cols = {
        'id_repository':df['id_repository'],
        'run_name':df['run_name'],
        'contaminated': df['Contaminated'],
        'n50': df['N50'],
        'yield':df['Yield'],
        'total_seqs':df['Read number'],
        # 'reads_mapped':df['Reads mapped'],
        'percent_mapped':df['Reads mapped (%)'],
        # 'primary_mapped':df['Primary mappings'],
        # 'secondary_mapped':df['Secondary mappings'],
        # 'supplementary_mapped':df['Supplementary mappings'],
        # 'reads_unmapped':df['Unmapped reads'],
        'median_read_quality':df['Median read quality'],
        'median_read_length':df['Median read length'],
        'chromosomal_depth':df['Chromosomal depth (mean)'],
        'total_depth':df['Total depth (mean)'],
        'snv':df['SNVs'],
        'indel':df['Indels'],
        'ts_tv':df['Transition/Transversion rate'],
        'sv_insertion':df['SV insertions'],
        'sv_deletion':df['SV deletions'],
        'sv_others':df['Other SVs'],
        'ploidy_estimation':df['Predicted sex chromosome karyotype'],
        # 'meta':df['meta'],
        # 'reads_at_least_0bp':df['Yield (reads >=0bp)'],
        # 'reads_at_least_5000bp':df['Yield (reads >=5000bp)'],
        # 'reads_at_least_10000bp':df['Yield (reads >=10000bp)'],
        # 'reads_at_least_15000bp':df['Yield (reads >=15000bp)'],
        # 'reads_at_least_20000bp':df['Yield (reads >=20000bp)'],
        # 'reads_at_least_25000bp':df['Yield (reads >=25000bp)'],
        # 'reads_at_least_30000bp':df['Yield (reads >=30000bp)'],
        # 'reads_at_least_35000bp':df['Yield (reads >=35000bp)'],
        # 'reads_at_least_40000bp':df['Yield (reads >=40000bp)'],
        # 'reads_at_least_45000bp':df['Yield (reads >=45000bp)'],
        # 'reads_at_least_50000bp':df['Yield (reads >=50000bp)'],
        # 'reads_at_least_55000bp':df[ 'Yield (reads >=55000bp)'],
        # 'reads_at_least_60000bp':df['Yield (reads >=60000bp)'],
        # 'reads_at_least_65000bp':df[ 'Yield (reads >=65000bp)'],
        # 'reads_at_least_70000bp':df['Yield (reads >=70000bp)'],
        # 'reads_at_least_75000bp':df['Yield (reads >=75000bp)'],
        # 'reads_at_least_80000bp':df['Yield (reads >=80000bp)'],
        # 'reads_at_least_85000bp':df['Yield (reads >=85000bp)'],
        # 'reads_at_least_90000bp':df['Yield (reads >=90000bp)'],
        # 'reads_at_least_95000bp':df['Yield (reads >=95000bp)'],
        # 'reads_at_least_100000bp':df['Yield (reads >=100000bp)'],
        # 'bases_at_least_1x':df['Bases with >=1-fold coverage'],
        # 'bases_at_least_10x':df['Bases with >=10-fold coverage'],
        # 'bases_at_least_15x':df['Bases with >=15-fold coverage'],
        # 'bases_at_least_20x':df['Bases with >=20-fold coverage'],
        # 'bases_at_least_30x':df['Bases with >=30-fold coverage'],
        'at_least_1x':df['Bases with >=1-fold coverage']/ 3200000000 * 100,
        'at_least_10x':df['Bases with >=10-fold coverage']/ 3200000000 * 100,
        'at_least_15x':df['Bases with >=15-fold coverage']/ 3200000000 * 100,
        'at_least_20x':df['Bases with >=20-fold coverage']/ 3200000000 * 100,
        'at_least_30x':df['Bases with >=30-fold coverage']/ 3200000000 * 100,

        # 'file_name':df['file_name']
    }

    # Construct the new DataFrame
    silver_df = pd.DataFrame(cols)
        
    # Convert cleaned DataFrame to CSV format
    csv_buffer = io.StringIO()
    silver_df.to_csv(csv_buffer, index=False)

    return csv_buffer.getvalue()

def upload_to_s3(cleaned_data, **kwargs):
    # Use data_interval_start for timestamp
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    s3_key = f'{prefix}{data_interval_start.isoformat()}.csv'

    # Use S3Hook to upload the cleaned CSV to S3
    s3 = S3Hook(aws_conn_id='aws')

    # Upload the file with timestamp in the name
    s3.load_string(
        string_data=cleaned_data,
        key=s3_key,
        bucket_name=S3_DWH_SILVER,
        replace=True
    )

    # Upload the same file as 'latest.csv'
    s3_key_latest = f'{prefix}latest.csv'
    s3.load_string(
        string_data=cleaned_data,
        key=s3_key_latest,
        bucket_name=S3_DWH_SILVER,
        replace=True
    )

# Define tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # To pass kwargs
    op_kwargs={'merged_data': '{{ task_instance.xcom_pull(task_ids="fetch_data") }}'},
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,  # To pass kwargs
    op_kwargs={'cleaned_data': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    dag=dag,
)

# Set task dependencies
fetch_data_task >> transform_data_task >> upload_to_s3_task