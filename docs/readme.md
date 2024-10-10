
## bronze-illumina-qc

Create nextflow.config

```
process {
    executor = 'awsbatch'
    queue = "bgsi-batch-small-queue"
}

aws {
    batch {
        cliPath = '/home/ec2-user/miniconda/bin/aws'
    }
    region = 'ap-southeast-3'
}
```

```
nextflow run bgsi-id/pl-mqc \
    -r main \
    -c nextflow.config \
    --dir_input s3://bgsi-data-dragen-qc/mqc/ \
    --config_mqc s3://bgsi-data-dev/SW/config_mqc_dragen.yml \
    --outdir s3://bgsi-data-dwh-bronze/illumina/qc/ \
    -work-dir s3://bgsi-data-dev/SW/mqc/ \
    -resume
```

## bronze-mgi-qc

Create nextflow.config

```
process {
    executor = 'awsbatch'
    queue = "bgsi-batch-small-queue"
}

aws {
    batch {
        cliPath = '/home/ec2-user/miniconda/bin/aws'
    }
    region = 'ap-southeast-3'
}
```

Run QC pipeline

```
nextflow run bgsi-id/pl-mqc \
    -r main \
    -c nextflow.config \
    --dir_input s3://bgsi-data-citus-qc/mqc/ \
    --config_mqc s3://bgsi-data-dev/SW/config_mqc_citus.yml \
    --outdir s3://bgsi-data-dwh-bronze/mgi/qc/ \
    -work-dir s3://bgsi-data-dev/SW/mqc/ \
    -resume
```

## silver-mgi-analysis

List output files

```
rclone lsjson s3:/bgsi-data-citus-output \
    --recursive \
    --fast-list \
    --include "*.cram*" \
    --include "*.vcf.gz*" > list_output_secondary_citus.json
```

Get input config files

```
rclone cp s3:/bgsi-data-citus-config ./config
rclone cp s3:/bgsi-config-pipeline/input/citus ./config-old
```

```python
import pandas as pd
import os

def file_type(fname):
    if fname.endswith('.cram'):
        return 'cram'
    elif fname.endswith('.cram.crai'):
        return 'cram_index'
    elif fname.endswith('.vcf.gz'):
        return 'vcf.gz'
    elif fname.endswith('.vcf.gz.tbi'):
        return 'vcf_index'
    
df = pd.read_json('list_output_secondary_citus.json')
df = df[df['IsDir'] == False]
df['id_repository'] = df['Name'].str.split('.').str[0]
df['run_name'] = df['Path'].str.split('/').str[0]
df['date_start'] = df['ModTime']
df['Path'] = 's3://bgsi-data-citus-output/' + df['Path']

df['file_type'] = df['Path'].apply(file_type)

pivot_table = df.pivot_table(index='id_repository', columns='file_type', values=['Path', 'Size'], aggfunc='first')
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
df_output =pivot_table[['id_repository', '' 'cram_size', 'cram', 'cram_index', 'vcf_size', 'vcf', 'vcf_index']]

df_output = df_output.merge(df, how='left', on='id_repository')
df_output = df_output[df_output['file_type'] == 'vcf.gz']

df_output['date_end'] = None
df_output['pipeline_name'] = 'CITUS'
df_output['pipeline_type'] = 'secondary'
df_output['run_status'] = 'SUCCEEDED'
df_output = df_output[['id_repository', 'date_start', 'date_end', 'pipeline_name', 'pipeline_type', 'run_name', 'run_status', 'cram_size', 'cram', 'cram_index', 'vcf_size', 'vcf', 'vcf_index']].reset_index(drop=True)

df_list = []
dirs = ['./config',
        './config-old']

for dir in dirs:
    for filename in os.listdir(dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(dir, filename)
            df = pd.read_csv(file_path)
            df_list.append(df)

df_input = pd.concat(df_list, ignore_index=True)
df_input = df_input[['ID', 'R1', 'R2']]
df_input['id_flowcell'] = df_input['R1'].str.split('/').str[-1].str.split('.fq.gz').str[0].str.split('_').str[0]
df_input['id_index'] = df_input['R1'].str.split('/').str[-1].str.split('.fq.gz').str[0].str.split('_').str[2]
df_input['id_repository'] = df_input['ID']
df_input['fastq_r1'] = df_input['R1']
df_input['fastq_r2'] = df_input['R2']

df_analysis = df_output.merge(df_input,how='left', on='id_repository')
df_analysis = df_analysis[[
    'id_repository', 'date_start', 'date_end', 'pipeline_name', 
    'pipeline_type', 'run_name', 'run_status',
    'id_flowcell', 'id_index',  
    'fastq_r1', 'fastq_r2',
    'cram_size', 'cram', 'cram_index', 'vcf_size', 'vcf', 'vcf_index']].reset_index(drop=True)
df_analysis.to_csv('latest.csv', index=False)
```