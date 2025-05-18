INSERT INTO ztronpro_analysis(id_repository,run_name,cram,cram_size,vcf,vcf_size,date_secondary,pipeline_name,pipeline_type,created_at,updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE cram=VALUES(cram),
cram_size=VALUES(cram_size),
vcf=VALUES(vcf),
vcf_size=VALUES(vcf_size),
date_secondary=VALUES(date_secondary),
pipeline_name=VALUES(pipeline_name),
pipeline_type=VALUES(pipeline_type),
updated_at=VALUES(updated_at)