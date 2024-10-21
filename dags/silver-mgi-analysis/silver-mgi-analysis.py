from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import pandas as pd
import json
from io import StringIO
import logging

S3_DWH_BRONZE = Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER = Variable.get("S3_DWH_SILVER")

prefix = "mgi/analysis/"

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver-mgi-analysis',
    default_args=default_args,
    description='Process CITUS data and generate latest.csv',
    schedule_interval=timedelta(days=1),
)

def fetch_s3_data(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket = S3_DWH_BRONZE

    try:
        # Fetch JSON data
        logging.info(f"Fetching JSON data from {bucket}/{prefix}")
        json_data = s3_hook.list_keys(bucket, prefix=prefix, delimiter='/')
        if not json_data:
            raise AirflowException(f"No files found in {bucket}/{prefix}")
        
        json_data = [
            {
                'Path': key, 
                'Name': key.split('/')[-1], 
                'Size': s3_hook.get_key(key, bucket).size, 
                'ModTime': s3_hook.get_key(key, bucket).last_modified.isoformat()
            } 
            for key in json_data 
            if key.endswith(('.cram', '.cram.crai', '.vcf.gz', '.vcf.gz.tbi'))
        ]
        
        if not json_data:
            raise AirflowException(f"No valid files found in {bucket}/{prefix}")

        # Fetch CSV data
        csv_files = {
            'config_data': 'bgsi-data-citus-config',
            'config_old_data': 'bgsi-config-pipeline/input/citus'
        }
        
        csv_data = {}
        for key, file_path in csv_files.items():
            logging.info(f"Fetching {key} from {bucket}/{file_path}")
            try:
                csv_data[key] = s3_hook.read_key(file_path, bucket)
            except Exception as e:
                logging.warning(f"Failed to fetch {key} from {bucket}/{file_path}: {str(e)}")
                csv_data[key] = None

        if all(value is None for value in csv_data.values()):
            raise AirflowException("Failed to fetch any CSV data")

        return {
            'json_data': json_data,
            **csv_data
        }

    except Exception as e:
        logging.error(f"Error in fetch_s3_data: {str(e)}")
        raise AirflowException(f"Failed to fetch S3 data: {str(e)}")

def process_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_s3_data')

    def file_type(fname):
        if fname.endswith('.cram'):
            return 'cram'
        elif fname.endswith('.cram.crai'):
            return 'cram_index'
        elif fname.endswith('.vcf.gz'):
            return 'vcf.gz'
        elif fname.endswith('.vcf.gz.tbi'):
            return 'vcf_index'

    # Process JSON data
    df = pd.DataFrame(data['json_data'])
    df = df[df['Path'].str.contains('/')]  # Equivalent to IsDir == False
    
    df['id_repository'] = df['Name'].str.split('.').str[0]
    df['run_name'] = df['Path'].str.split('/').str[0]
    df['date_start'] = pd.to_datetime(df['ModTime'])
    df['Path'] = f's3://{S3_DWH_BRONZE}/{prefix}' + df['Path']
    df['file_type'] = df['Path'].apply(file_type)

    df_sorted = df.sort_values(['id_repository', 'file_type', 'Size', 'ModTime'], 
                               ascending=[True, True, False, False])
    df_deduplicated = df_sorted.drop_duplicates(subset=['id_repository', 'file_type'], keep='first')

    pivot_table = df_deduplicated.pivot_table(index='id_repository', columns='file_type', values=['Path', 'Size'], aggfunc='first')
    pivot_table.columns = [f'{col[1]}_{col[0]}' for col in pivot_table.columns]
    pivot_table = pivot_table.rename(columns={
        'cram_Path': 'cram',
        'cram_Size': 'cram_size',
        'cram_index_Path': 'cram_index',
        'cram_index_Size': 'cram_index_size',
        'vcf.gz_Path': 'vcf',
        'vcf.gz_Size': 'vcf_size',
        'vcf_index_Path': 'vcf_index',
        'vcf_index_Size': 'vcf_index_size'
    })

    pivot_table.reset_index(inplace=True)
    df_output = pivot_table[['id_repository', 'cram_size', 'cram', 'cram_index', 'vcf_size', 'vcf', 'vcf_index']]

    df_output = df_output.merge(df_deduplicated, how='left', on='id_repository')
    df_output = df_output[df_output['file_type'] == 'vcf.gz']

    df_output['date_end'] = None
    df_output['pipeline_name'] = 'CITUS'
    df_output['pipeline_type'] = 'secondary'
    df_output['run_status'] = 'SUCCEEDED'
    df_output = df_output[['id_repository', 'date_start', 'date_end', 'pipeline_name', 'pipeline_type', 'run_name', 'run_status', 'cram_size', 'cram', 'cram_index', 'vcf_size', 'vcf', 'vcf_index']].reset_index(drop=True)

    # Process CSV data
    df_list = []
    for csv_data in [data['config_data'], data['config_old_data']]:
        if csv_data is not None:
            df = pd.read_csv(StringIO(csv_data))
            expected_columns = ['ID', 'R1', 'R2']
            if set(expected_columns).issubset(df.columns):
                df = df.drop_duplicates(subset='ID', keep='first')
                df_list.append(df)

    if not df_list:
        raise ValueError("No valid CSV data was found or processed.")

    df_input = pd.concat(df_list, ignore_index=True)
    df_input = df_input.drop_duplicates(subset='ID', keep='first')

    df_input = df_input[['ID', 'R1', 'R2']]
    df_input['id_flowcell'] = df_input['R1'].str.split('/').str[-1].str.split('.fq.gz').str[0].str.split('_').str[0]
    df_input['id_index'] = df_input['R1'].str.split('/').str[-1].str.split('.fq.gz').str[0].str.split('_').str[2]
    df_input['id_repository'] = df_input['ID']
    df_input['fastq_r1'] = df_input['R1']
    df_input['fastq_r2'] = df_input['R2']

    # Merge df_output and df_input, ensuring no duplicates
    df_analysis = df_output.merge(df_input, how='left', on='id_repository')
    df_analysis = df_analysis.drop_duplicates(subset='id_repository', keep='first')

    df_analysis = df_analysis[[
        'id_repository', 'date_start', 'date_end', 'pipeline_name', 
        'pipeline_type', 'run_name', 'run_status',
        'id_flowcell', 'id_index',  
        'fastq_r1', 'fastq_r2',
        'cram_size', 'cram', 'cram_index', 'vcf_size', 'vcf', 'vcf_index']].reset_index(drop=True)

    return df_analysis.to_csv(index=False)

def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='process_data')
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Generate timestamp for the filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{prefix}mgi_analysis_{timestamp}.csv"
    
    # Upload the file with timestamp in the name
    s3_hook.load_string(
        string_data=cleaned_data,
        key=s3_key,
        bucket_name=S3_DWH_SILVER,
        replace=True
    )

    # Upload the same file as 'latest.csv'
    s3_key_latest = f"{prefix}latest.csv"
    s3_hook.load_string(
        string_data=cleaned_data,
        key=s3_key_latest,
        bucket_name=S3_DWH_SILVER,
        replace=True
    )

def log_fetch_results(**kwargs):
    ti = kwargs['ti']
    results = ti.xcom_pull(task_ids='fetch_s3_data')
    logging.info(f"Fetch results: {results}")

with dag:
    fetch_task = PythonOperator(
        task_id='fetch_s3_data',
        python_callable=fetch_s3_data,
    )

    log_fetch_results_task = PythonOperator(
        task_id='log_fetch_results',
        python_callable=log_fetch_results,
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )

    fetch_task >> log_fetch_results_task >> process_task >> upload_to_s3_task