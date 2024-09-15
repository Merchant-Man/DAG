

import os
from datetime import datetime, timedelta
import json
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

S3_DWH_BRONZE=Variable.get("S3_DWH_BRONZE")
S3_DWH_SILVER=Variable.get("S3_DWH_SILVER")

default_args = {
    'owner': 'bgsi-data',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver-bioinformatics',
    default_args=default_args,
    description='An ETL pipeline to fetch data from an API, transform using Pandas, and load to S3',
    schedule_interval=timedelta(days=1),
)

def fetch_bronze_samples_zlims(**kwargs):
    hook = S3Hook('aws')
    files = hook.list_keys(prefix='samples/zlims', bucket_name=S3_DWH_BRONZE)
    paths = [] 
    for file_key in files:
        filename = os.path.basename(file_key)
        print(filename)
        paths.append(hook.download_file(key=file_key, bucket_name=S3_DWH_BRONZE))
    return paths

def fetch_bronze_samples_bfx(**kwargs):
    hook = S3Hook('aws')
    files = hook.list_keys(prefix='samples/bfx', bucket_name=S3_DWH_BRONZE)
    paths = [] 
    for file_key in files:
        filename = os.path.basename(file_key)
        print(filename)
        paths.append(hook.download_file(key=file_key, bucket_name=S3_DWH_BRONZE))
    return paths

def fetch_bronze_analysis_bfx(**kwargs):
    hook = S3Hook('aws')
    files = hook.list_keys(prefix='analysis/bfx', bucket_name=S3_DWH_BRONZE)
    paths = [] 
    for file_key in files:
        filename = os.path.basename(file_key)
        print(filename)
        paths.append(hook.download_file(key=file_key, bucket_name=S3_DWH_BRONZE))
    return paths

def fetch_bronze_samples_ica(**kwargs):
    hook = S3Hook('aws')
    files = hook.list_keys(prefix='samples/ica', bucket_name=S3_DWH_BRONZE)
    paths = [] 
    for file_key in files:
        filename = os.path.basename(file_key)
        print(filename)
        paths.append(hook.download_file(key=file_key, bucket_name=S3_DWH_BRONZE))
    return paths

def fetch_bronze_analysis_ica(**kwargs):
    hook = S3Hook('aws')
    files = hook.list_keys(prefix='analysis/ica', bucket_name=S3_DWH_BRONZE)
    paths = [] 
    for file_key in files:
        filename = os.path.basename(file_key)
        print(filename)
        paths.append(hook.download_file(key=file_key, bucket_name=S3_DWH_BRONZE))
    return paths

def fetch_bronze_qc(**kwargs):
    hook = S3Hook('aws')
    files = hook.list_keys(prefix='qc', bucket_name=S3_DWH_BRONZE)
    paths = [] 
    for file_key in files:
        filename = os.path.basename(file_key)
        print(filename)
        paths.append(hook.download_file(key=file_key, bucket_name=S3_DWH_BRONZE))
    return paths

def transform_bronze_samples_zlims(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    paths = kwargs['ti'].xcom_pull(task_ids=['fetch_bronze_samples_zlims'])
    
    df_list = []
    for file in paths[0]:
        df = pd.read_csv(file)
        df_list.append(df)
    combined_df = pd.concat(df_list)
    combined_df = combined_df.drop_duplicates()

    transformed_df = pd.DataFrame()
    # transformed_df['uuid'] = combined_df['id']
    transformed_df['id_sample'] = combined_df['Sample ID(*)']
    transformed_df['id_flowcell'] = combined_df['Flowcell ID']
    transformed_df['create_by'] = 'zlims'
    transformed_df['create_org'] = 'bgsi'
    transformed_df['create_date'] = combined_df['Create Time']
    transformed_df['modification_date'] = combined_df['Create Time']
    transformed_df['species'] = 'Homo sapiens'
    transformed_df['library_strategy'] = 'WGS'
    transformed_df['platform'] = 'MGI'
    transformed_df['instrument_model'] = 'DNBSEQ-T7'
    # transformed_df['note'] = combined_df['status']

    # ASSUMPTION
    transformed_df['sequencing'] = 'True'
    transformed_df['transfer_data'] = 'True'
    transformed_df['analysis_primary'] = 'True'

    transformed_df.to_csv(f'/tmp/samples-zlims-merge-{data_interval_start.isoformat()}.csv', index=False)

def transform_bronze_samples_bfx(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    paths = kwargs['ti'].xcom_pull(task_ids=['fetch_bronze_samples_bfx'])
    
    df_list = []
    for file in paths[0]:
        df = pd.read_csv(file)
        df_list.append(df)
    combined_df = pd.concat(df_list)
    combined_df = combined_df.drop_duplicates()

    combined_df['sequencing'] = 'True'
    combined_df['transfer_data'] = 'True'
    combined_df['analysis_primary'] = 'True'
    combined_df.rename(columns={'raw_data': 'analysis_primary_data'}, inplace=True)

    combined_df.to_csv(f'/tmp/samples-bfx-merge-{data_interval_start.isoformat()}.csv', index=False)

def transform_bronze_analysis_bfx(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    paths = kwargs['ti'].xcom_pull(task_ids=['fetch_bronze_analysis_bfx'])
    
    df_list = []
    for file in paths[0]:
        df = pd.read_csv(file)
        df_list.append(df)
    combined_df = pd.concat(df_list)
    combined_df = combined_df.drop_duplicates()

    combined_df.loc[combined_df['pipeline'] == 'citus', 'analysis_secondary'] = 'True'
    combined_df.loc[combined_df['pipeline'] == 'citus', 'analysis_secondary_name'] = combined_df['id_sample']
    combined_df.loc[combined_df['pipeline'] == 'citus', 'analysis_secondary_data'] = combined_df['analysis_data']
    combined_df.loc[combined_df['pipeline'] == 'citus', 'analysis_secondary_date'] = combined_df['modification_date']

    combined_df.to_csv(f'/tmp/analysis-bfx-merge-{data_interval_start.isoformat()}.csv', index=False)

def transform_bronze_samples_ica(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    paths = kwargs['ti'].xcom_pull(task_ids=['fetch_bronze_samples_ica'])
    
    df_list = []
    for file in paths[0]:
        df = pd.read_csv(file)
        df_list.append(df)
    combined_df = pd.concat(df_list)
    combined_df = combined_df.drop_duplicates()
    
    transformed_df = pd.DataFrame()
    transformed_df['uuid'] = combined_df['id']
    transformed_df['create_by'] = 'ica'
    transformed_df['create_org'] = combined_df['tenantName']
    transformed_df['create_date'] = combined_df['timeCreated']
    transformed_df['modification_date'] = combined_df['timeModified']
    transformed_df['id_sample'] = combined_df['name']
    transformed_df['species'] = 'Homo sapiens'
    transformed_df['library_strategy'] = 'WGS'
    transformed_df['platform'] = 'Illumina'
    transformed_df['instrument_model'] = 'NovaSeq 6000'
    transformed_df['note'] = combined_df['status']

    # ASSUMPTION
    transformed_df['sequencing'] = 'True'
    transformed_df['transfer_data'] = 'True'
    transformed_df['analysis_primary'] = 'True'

    transformed_df.to_csv(f'/tmp/samples-ica-merge-{data_interval_start.isoformat()}.csv', index=False)

def transform_bronze_analysis_ica(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    paths = kwargs['ti'].xcom_pull(task_ids=['fetch_bronze_analysis_ica'])
    
    df_list = []
    for file in paths[0]:
        df = pd.read_csv(file)
        df_list.append(df)
    combined_df = pd.concat(df_list)
    combined_df = combined_df.drop_duplicates()
    combined_df = combined_df[combined_df['status'] == 'SUCCEEDED']

    transformed_df = pd.DataFrame()
    transformed_df['id_sample'] = combined_df['userReference'].apply(lambda x: x.split('_')[0])
    transformed_df['analysis_secondary'] = 'True'
    transformed_df['analysis_secondary_name'] = combined_df['userReference']
    transformed_df['analysis_secondary_data'] = None
    transformed_df['analysis_secondary_date'] = combined_df['timeModified']

    # transformed_df['uuid'] = combined_df['id']
    # transformed_df['create_by'] = 'ica'
    # transformed_df['create_org'] = combined_df['tenantName']
    # transformed_df['create_date'] = combined_df['timeCreated']
    # transformed_df['modification_date'] = combined_df['timeModified']
    # transformed_df['sample'] = combined_df['userReference'].apply(lambda x: x.split('-')[0])
    # transformed_df['pipeline'] = combined_df['pipeline'].apply(lambda x: eval(x)['code'])
    # transformed_df['analysis_primary'] = 'secondary'
    # transformed_df['status'] = combined_df['status']
    # transformed_df['analysis_data'] = ''
        
    transformed_df.to_csv(f'/tmp/analysis-ica-merge-{data_interval_start.isoformat()}.csv', index=False)

def transform_bronze_qc(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start
    paths = kwargs['ti'].xcom_pull(task_ids=['fetch_bronze_qc'])
    
    df_list = []
    for file in paths[0]:
        df = pd.read_csv(file)
        df_list.append(df)
    combined_df = pd.concat(df_list)
    combined_df = combined_df.drop_duplicates()

    combined_df.to_csv(f'/tmp/qc-merge-{data_interval_start.isoformat()}.csv', index=False)

def transform_bronze(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start

    df_samples_zlims = pd.read_csv(f'/tmp/samples-zlims-merge-{data_interval_start.isoformat()}.csv')
    df_samples_bfx   = pd.read_csv(f'/tmp/samples-bfx-merge-{data_interval_start.isoformat()}.csv')
    df_analysis_bfx     = pd.read_csv(f'/tmp/analysis-bfx-merge-{data_interval_start.isoformat()}.csv')
    df_samples_ica   = pd.read_csv(f'/tmp/samples-ica-merge-{data_interval_start.isoformat()}.csv')
    df_analysis_ica     = pd.read_csv(f'/tmp/analysis-ica-merge-{data_interval_start.isoformat()}.csv')
    df_qc               = pd.read_csv(f'/tmp/qc-merge-{data_interval_start.isoformat()}.csv')

    # df_analysis = df_analysis_ica
    df_analysis = pd.concat([df_analysis_bfx, df_analysis_ica])
    df_samples = pd.concat([df_samples_bfx, df_samples_ica, df_samples_zlims])
    df_samples.to_csv(f'/tmp/debug-{data_interval_start}.csv', index=False)

    # df_bioinformatics = df_samples
    df_bioinformatics = df_samples.merge(df_analysis, on='id_sample', how='left')
    df_bioinformatics = df_bioinformatics.merge(df_qc, on='id_sample', how='left')

    df_bioinformatics.to_csv(f'/tmp/bioinformatics-{data_interval_start}.csv', index=False)

def load_s3(**kwargs):
    data_interval_start = kwargs['ti'].get_dagrun().data_interval_start

    upload_to_s3_task = LocalFilesystemToS3Operator(
        task_id='upload_to_s3_original',
        filename=f'/tmp/bioinformatics-{data_interval_start}.csv',
        dest_key=f'bioinformatics-{data_interval_start}.csv',
        dest_bucket=S3_DWH_SILVER,
        aws_conn_id='aws',
        dag=dag,
    )

    upload_to_s3_task_latest = LocalFilesystemToS3Operator(
        task_id='upload_to_s3_latest',
        filename=f'/tmp/bioinformatics-{data_interval_start}.csv',
        dest_key='latest.csv',
        replace=True,
        dest_bucket=S3_DWH_SILVER,
        aws_conn_id='aws',
        dag=dag,
    )

    return upload_to_s3_task_latest.execute(context=kwargs)

fetch_bronze_samples_zlims_op = PythonOperator(
    task_id='fetch_bronze_samples_zlims',
    python_callable=fetch_bronze_samples_zlims,
    provide_context=True,
    dag=dag,
)

fetch_bronze_samples_bfx_op = PythonOperator(
    task_id='fetch_bronze_samples_bfx',
    python_callable=fetch_bronze_samples_bfx,
    provide_context=True,
    dag=dag,
)

fetch_bronze_samples_ica_op = PythonOperator(
    task_id='fetch_bronze_samples_ica',
    python_callable=fetch_bronze_samples_ica,
    provide_context=True,
    dag=dag,
)

fetch_bronze_analysis_bfx_op = PythonOperator(
    task_id='fetch_bronze_analysis_bfx',
    python_callable=fetch_bronze_analysis_bfx,
    provide_context=True,
    dag=dag,
)

fetch_bronze_analysis_ica_op = PythonOperator(
    task_id='fetch_bronze_analysis_ica',
    python_callable=fetch_bronze_analysis_ica,
    provide_context=True,
    dag=dag,
)

fetch_bronze_qc_op = PythonOperator(
    task_id='fetch_bronze_qc',
    python_callable=fetch_bronze_qc,
    provide_context=True,
    dag=dag,
)

transform_bronze_samples_zlims_op = PythonOperator(
    task_id='transform_bronze_samples_zlims',
    python_callable=transform_bronze_samples_zlims,
    provide_context=True,
    dag=dag,
)

transform_bronze_samples_bfx_op = PythonOperator(
    task_id='transform_bronze_samples_bfx',
    python_callable=transform_bronze_samples_bfx,
    provide_context=True,
    dag=dag,
)

transform_bronze_analysis_bfx_op = PythonOperator(
    task_id='transform_bronze_analysis_bfx',
    python_callable=transform_bronze_analysis_bfx,
    provide_context=True,
    dag=dag,
)

transform_bronze_samples_ica_op = PythonOperator(
    task_id='transform_bronze_samples_ica',
    python_callable=transform_bronze_samples_ica,
    provide_context=True,
    dag=dag,
)

transform_bronze_analysis_ica_op = PythonOperator(
    task_id='transform_bronze_analysis_ica',
    python_callable=transform_bronze_analysis_ica,
    provide_context=True,
    dag=dag,
)

transform_bronze_qc_op = PythonOperator(
    task_id='transform_bronze_qc',
    python_callable=transform_bronze_qc,
    provide_context=True,
    dag=dag,
)

transform_bronze_op = PythonOperator(
    task_id='transform_bronze',
    python_callable=transform_bronze,
    provide_context=True,
    dag=dag,
)

load_s3_op = PythonOperator(
    task_id='load_s3',
    python_callable=load_s3,
    provide_context=True,
    dag=dag,
)

fetch_bronze_samples_zlims_op >> transform_bronze_samples_zlims_op >> transform_bronze_op >> load_s3_op
fetch_bronze_samples_bfx_op >> transform_bronze_samples_bfx_op >> transform_bronze_op >> load_s3_op
fetch_bronze_analysis_bfx_op >> transform_bronze_analysis_bfx_op >> transform_bronze_op >> load_s3_op
fetch_bronze_samples_ica_op >> transform_bronze_samples_ica_op >> transform_bronze_op >> load_s3_op
fetch_bronze_analysis_ica_op >> transform_bronze_analysis_ica_op >> transform_bronze_op >> load_s3_op
fetch_bronze_qc_op >> transform_bronze_qc_op >> transform_bronze_op >> load_s3_op