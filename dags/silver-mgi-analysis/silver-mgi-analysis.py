from airflow.exceptions import AirflowException
import logging

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
