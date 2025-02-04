INSERT INTO wfhv_analysis(id_repository,run_name,cram,cram_size,vcf,vcf_size,pipeline_name,pipeline_type,date_start,created_at,updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE id_repository = VALUES(id_repository),
run_name = VALUES(run_name),
cram = VALUES(cram),
cram_size = VALUES(cram_size),
vcf = VALUES(vcf),
vcf_size = VALUES(vcf_size),
pipeline_name = VALUES(pipeline_name),
pipeline_type = VALUES(pipeline_type),
date_start = VALUES(date_start),
updated_at = '{{ ts }}'