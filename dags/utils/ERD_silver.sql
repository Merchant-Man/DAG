CREATE TABLE regina_demography (
  id_subject VARCHAR(50) PRIMARY KEY comment 'Encrypted subject ID from RegINA',
  age TINYINT UNSIGNED NOT NULL,
  sex VARCHAR(6) NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  creationDate DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar',
  ehr_id VARCHAR(36) NOT NULL comment 'EHR ID from SatuSehat'
);


CREATE TABLE phenovar_participants (
  id_subject VARCHAR(10) PRIMARY KEY comment 'Encrypted subject ID from PhenoVar',
  encrypt_full_name VARCHAR(225) NOT NULL comment 'Encrypted full name from PhenoVar',
  encrypt_nik VARCHAR(225) NOT NULL comment 'Encrypted NIK from PhenoVar', 
  encrypt_birth_date VARCHAR(225) NOT NULL comment 'Encrypted birth date from PhenoVar',
  sex VARCHAR(6) NOT NULL,
  source VARCHAR(64) comment 'Source of data i.e. SatuSehat',
  province_code VARCHAR(2) NOT NULL comment 'Province code from NIK',
  province VARCHAR(64) NOT NULL,
  district_code VARCHAR(4) NOT NULL comment 'District (Kab.) code from NIK',
  district VARCHAR(64) NOT NULL,
  subdistrict_code VARCHAR(4) NOT NULL comment 'Subdistrict (Kec.) code from NIK',
  subdistrict VARCHAR(64) NOT NULL,
  village_code VARCHAR(4) NOT NULL comment 'Subdistrict (Kec.) code from NIK',
  village VARCHAR(64) NOT NULL,
  use_nik_ibu BOOLEAN NOT NULL,
  created_by VARCHAR(128) NOT NULL,
  updated_by VARCHAR(128) NOT NULL,
  hospital_name VARCHAR(128) NOT NULL comment 'Hospital name extracted from the creator email',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  creation_date DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar',
  updation_date DATETIME NOT NULL comment 'Timestamp of last update from PhenoVar',
  encrypt_ihs_number VARCHAR(64) NOT NULL commnet 'Encrypted IHS number from SatuSehat'
);
CREATE INDEX hospital_name_idx
ON phenovar_participants(hospital_name);
CREATE INDEX sex_idx
ON phenovar_participants(sex);
CREATE INDEX province_code_idx
ON phenovar_participants(province_code);


CREATE TABLE master_specimen (
id VARCHAR(30) UNSIGNED PRIMARY KEY comment 'ID of the speciment', --biosample_specimen
id_type_specimen	VARCHAR(8) NOT NULL,
specimen_type_name	VARCHAR(32) NOT NULL,
specimen_kode VARCHAR(8), 	
specimen_type_repo VARCHAR(8),
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
creationDate DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar'
);


CREATE TABLE master_sample_type (
id SMALLINT UNSIGNED PRIMARY KEY,
sample_name	VARCHAR(32),
active	BOOLEAN, 
id_biobank VARCHAR(128),
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
creationDate DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar'
);


CREATE TABLE master_status (
id SMALLINT UNSIGNED PRIMARY KEY,
status_name	VARCHAR(32),
status_color	VARCHAR(32),
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
creationDate DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar'
);


CREATE TABLE master_biobank (
id_biobank VARCHAR(128) PRIMARY KEY,
biobank_tree VARCHAR(64), 
biobank_nama VARCHAR(255),
biobank_code VARCHAR(32),
biobank_no_kontak VARCHAR(50),
biobank_email VARCHAR(100),
biobank_alamat text,
biobank_penanggung_jawab VARCHAR(100),
biobank_repo VARCHAR(8),
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
creationDate DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar'
);
	

CREATE TABLE  simbiox_patients(
id_patient VARCHAR(128) PRIMARY KEY  comment'ID (PK) of the record from simbiox patient data',
id_mpi VARCHAR(125),
id_subject VARCHAR(50) comment 'nomor_mr (medical record) from simbiox data. The data shoul be filled either by Phenovar or RegINA ID',
sex VARCHAR(6), 
date_of_birth DATE comment 'Date of birth of the patient',
id_biobank VARCHAR(128) comment 'ID of the biobank',
created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
ALTER TABLE simbiox_patients
ADD INDEX id_subject_idx (id_subject);
ALTER TABLE simbiox_patients
ADD INDEX `id_biobank_idx` (`id_biobank`);
	


CREATE TABLE  simbiox_biosamples(
id VARCHAR(64) PRIMARY KEY comment 'ID (PK) Uniqueness from simbiox',
id_patient VARCHAR(128) comment'ID of the record from simbiox patient data',
code_repository VARCHAR(32) comment 'Code of the repository',
code_box VARCHAR(10),
code_position VARCHAR(10),
date_received DATETIME, 
date_enumerated DATETIME,
id_biobank VARCHAR(64),
origin_code_repository VARCHAR(32),
origin_code_box VARCHAR(10),
biosample_type VARCHAR(16),
biosample_specimen VARCHAR(16),
type_case VARCHAR(16),
sub_cell_specimen VARCHAR(16),
biosample_volume MEDIUMINT UNSIGNED,
biosample_status VARCHAR(16),
created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
final_result_analytic VARCHAR(16),
tube_type VARCHAR(3),
degrees_celsius_storage SMALLINT,
research_id VARCHAR(36),
project_id VARCHAR(36),
last_save DATETIME,
`remove` VARCHAR(2),
log_biospc_id VARCHAR(36),
kode_kedatangan VARCHAR(36),
kode_hub_penyimpan VARCHAR(36)
);
CREATE INDEX id_patient_idx
ON simbiox_biosamples (id_patient);
CREATE INDEX code_repository_idx
ON simbiox_biosamples (code_repository);
CREATE INDEX origin_code_repository_idx
ON simbiox_biosamples (origin_code_repository);
CREATE INDEX id_patient_code_repository_idx
ON simbiox_biosamples (id_patient, code_repository);
CREATE INDEX kode_kedatangan_idx
ON simbiox_biosamples (kode_kedatangan);
CREATE INDEX kode_hub_penyimpan_idx
ON simbiox_biosamples (kode_hub_penyimpan);


CREATE TABLE ica_samples(
  id VARCHAR(128) PRIMARY KEY comment 'ID of the record from ICA samples',
  id_library VARCHAR(32) comment 'ID of the library of the sample',
  time_created DATETIME comment 'Timestamp when the record is created on ICA side',
  time_modified DATETIME comment 'Timestamp when the record is modified on ICA side',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  owner_id VARCHAR(128), 
  tenant_id VARCHAR(128),
  tenant_name VARCHAR(128),
  id_repository VARCHAR(128),
  `status` VARCHAR(32),
  tag_technical_tags TEXT,
  tag_user_tags TEXT,
  tag_connetor_tags TEXT,
  tag_run_in_tags TEXT,
  application_id VARCHAR(128),
  application_name VARCHAR(128),
  sample_list_technical_tags TEXT,
  sample_list_user_tags TEXT,
  sample_list_connector_tags TEXT,
  sample_list_run_in_tags TEXT,
  sample_list_run_out_tags TEXT,
  sample_list_reference_tags TEXT
);
CREATE INDEX id_library_idx
ON ica_samples (id_library);
CREATE INDEX id_repository_idx
ON ica_samples (id_repository);

-- Already check for illumina_qc id_repository should be unique
CREATE TABLE illumina_qc(
  id_repository VARCHAR(32) comment 'Code of the repository',
  percent_dups FLOAT,
  percent_q30_bases FLOAT, 
  total_seqs DOUBLE,
  contam FLOAT,
  non_primary FLOAT,
  percent_mapped FLOAT,
  percent_proper_pairs FLOAT,
  reads_mapped DOUBLE,
  at_least_50x FLOAT,
  at_least_20x FLOAT,
  at_least_10x FLOAT,
  median_coverage FLOAT,
  vars INT UNSIGNED,
  snp FLOAT,
  indel DOUBLE,
  ts_tv FLOAT,
  depth FLOAT,
  ploidy_estimation VARCHAR(6),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  run_name VARCHAR(255)
  PRIMARY KEY (id_repository, run_name)
);
CREATE INDEX id_repository_idx
ON illumina_qc(id_repository);
CREATE INDEX run_name_idx
ON illumina_qc(run_name);

-- for QS, combination of id_repository,lane,read_number,yield should be unique
-- index should not be used since it is primer bases sequence index. 
CREATE TABLE illumina_qs(
  id_repository VARCHAR(32) comment 'Code of the repository',
  lane TINYINT UNSIGNED,
  `index` VARCHAR(32),
  index2 VARCHAR(32),
  read_number TINYINT UNSIGNED,
  yield BIGINT UNSIGNED,
  yield_q30 BIGINT UNSIGNED,
  quality_score_sum DOUBLE,
  mean_quality_score_pf DOUBLE,
  percent_q30 FLOAT, 
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_repository_idx
ON illumina_qs(id_repository);

ALTER TABLE illumina_qs
ADD CONSTRAINT PK_qs PRIMARY KEY (id_repository,lane,read_number,yield);




CREATE TABLE mgi_qc(
  id_repository VARCHAR(32) comment 'Code of the repository',
  percent_dups DOUBLE,
  percent_gc FLOAT,
  total_seqs DOUBLE,
  non_primary FLOAT,
  percent_mapped DOUBLE,
  percent_proper_pairs DOUBLE,
  reads_mapped DOUBLE,
  at_least_50x FLOAT,
  at_least_20x FLOAT,
  at_least_10x FLOAT,
  median_coverage FLOAT,
  depth FLOAT,
  vars INT UNSIGNED,
  snp FLOAT,
  indel DOUBLE,
  ts_tv FLOAT,
  ploidy_estimation VARCHAR(6),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  run_name VARCHAR(255),
  PRIMARY KEY (id_repository, run_name)
);
CREATE INDEX id_repository_idx
ON mgi_qc(id_repository);
CREATE INDEX run_name_idx
ON mgi_qc(run_name);
-- ALTER TABLE mgi_qc
-- ADD CONSTRAINT PK_qs PRIMARY KEY (id_repository, ploidy_estimation, vars, snp, indel, depth, percent_dups, median_coverage)


CREATE TABLE mgi_analysis(
  id_repository VARCHAR(32) comment 'Code of the repository',
  date_start DATETIME,
  date_end DATETIME,
  pipeline_name VARCHAR(255),
  pipeline_type VARCHAR(255),
  run_name VARCHAR(255),
  run_status VARCHAR(32),
  id_flowcell VARCHAR(32),
  id_index VARCHAR(32),
  fastq_r1 TEXT,
  fastq_r2 TEXT,
  cram_size BIGINT UNSIGNED,
  cram TEXT,
  cram_index TEXT,
  vcf_size BIGINT UNSIGNED,
  vcf TEXT,
  vcf_index TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_repository_idx
ON mgi_analysis(id_repository);
CREATE INDEX run_status_idx
ON mgi_analysis(run_status);
ALTER TABLE mgi_analysis
ADD CONSTRAINT PK_analysis PRIMARY KEY (id_repository, run_name);


CREATE TABLE wfhv_qc(
  id_repository VARCHAR(32) comment 'Code of the repository',
  run_name VARCHAR(255),
  contaminated FLOAT,
  n50 BIGINT UNSIGNED,
  yield BIGINT UNSIGNED,
  total_seqs BIGINT UNSIGNED,
  percent_mapped DOUBLE,
  median_read_quality DOUBLE,
  median_read_length DOUBLE,
  chromosomal_depth DOUBLE,
  total_depth FLOAT,
  snv INT UNSIGNED,
  indel INT UNSIGNED,
  ts_tv FLOAT,
  sv_insertion INT UNSIGNED,
  sv_deletion INT UNSIGNED,
  sv_others INT UNSIGNED,
  ploidy_estimation VARCHAR(6),
  at_least_1x DOUBLE,
  at_least_10x DOUBLE,
  at_least_15x DOUBLE,
  at_least_20x DOUBLE,
  at_least_30x DOUBLE,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_repository_idx
ON wfhv_qc(id_repository);
ALTER TABLE wfhv_qc
ADD CONSTRAINT PK_qc PRIMARY KEY (id_repository, run_name);



CREATE TABLE wfhv_analysis(
  id_repository VARCHAR(32) comment 'Code of the repository',
  run_name VARCHAR(255),
  cram TEXT,
  cram_size BIGINT UNSIGNED,
  vcf TEXT,
  vcf_size BIGINT UNSIGNED,
  pipeline_name VARCHAR(255),
  pipeline_type VARCHAR(255),
  date_start DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_repository_idx
ON wfhv_analysis(id_repository);
ALTER TABLE wfhv_analysis
ADD CONSTRAINT PK_analysis PRIMARY KEY (id_repository, run_name);



CREATE TABLE wfhv_samples(
  id_repository VARCHAR(32) PRIMARY KEY comment 'Code of the repository',
  alias TEXT,
  total_passed_bases TEXT,
  bam_size TEXT,
  date_upload TEXT,
  total_bases TEXT,
  passed_bases_percent TEXT,
  bam_folder TEXT,
  id_library TEXT,
  sum_of_total_passed_bases TEXT,
  sum_of_bam_size TEXT,
  id_batch VARCHAR(255),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);


CREATE TABLE zlims_samples(
  id_repository VARCHAR(32) comment 'Code of the repository',
  id_flowcell VARCHAR(32),
  id_pool VARCHAR(64),
  id_dnb VARCHAR(64),
  id_index INT UNSIGNED,
  date_create DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_repository_idx
ON zlims_samples(id_repository);
ALTER TABLE zlims_samples
ADD CONSTRAINT PK_sampeles PRIMARY KEY (id_repository, id_flowcell, id_pool, id_dnb, id_index);


CREATE TABLE ica_analysis (
  `id` varchar(128)  PRIMARY KEY,
  time_created datetime,
  time_modified datetime,
  created_at datetime,
  updated_at datetime,
  id_repository varchar(128),
  id_batch varchar(128),
  date_start datetime,
  date_end datetime,
  pipeline_name varchar(255),
  pipeline_type varchar(255),
  run_name varchar(255),
  run_status varchar(32),
  cram text,
  cram_size bigint UNSIGNED,
  vcf text,
  vcf_size bigint UNSIGNED,
  tag_technical_tags text,
  tag_user_tags text,
  tag_reference_tags text
);
CREATE INDEX id_repository_idx
ON ica_analysis(id_repository);
CREATE INDEX run_status_idx
ON ica_analysis(run_status);

CREATE TABLE staging_illumina_sec (
	date_start datetime,
	id_repository varchar(32),
	id_batch varchar(32),
	pipeline_name varchar(255),
	run_name varchar(255),
	cram text,
	cram_size bigint UNSIGNED,
	vcf text,
	vcf_size bigint UNSIGNED,
	tag_user_tags text,
	percent_dups float,
	percent_q30_bases float,
	total_seqs double,
	median_coverage float,
	contamination float,
	at_least_10x float,
	at_least_20x float,
	ploidy_estimation varchar(6),
	snp float,
	indel double,
	ts_tv float,
	yield decimal(42, 0),
	yield_q30 decimal(42, 0)
);
CREATE INDEX id_repository_idx
ON staging_illumina_sec(id_repository);

CREATE TABLE staging_mgi_sec (
	date_start datetime,
	id_repository varchar(32),
	id_batch varchar(32),
	pipeline_name varchar(255),
	run_name varchar(255),
	cram text,
	cram_size bigint UNSIGNED,
	vcf text,
	vcf_size bigint UNSIGNED,
	tag_user_tags binary(0),
	percent_dups double,
	percent_q30_bases binary(0),
	total_seqs double,
	median_coverage float,
	contamination binary(0),
	at_least_10x float,
	at_least_20x float,
	ploidy_estimation varchar(2),
	snp float,
	indel double,
	ts_tv float
);
CREATE INDEX id_repository_idx
ON staging_mgi_sec(id_repository);

CREATE TABLE staging_ont_sec (
	date_start datetime,
	id_repository varchar(32),
	id_batch binary(0),
	pipeline_name varchar(255),
	run_name varchar(255),
	cram text,
	cram_size bigint UNSIGNED,
	vcf text,
	vcf_size bigint UNSIGNED,
	tag_user_tags binary(0),
	percent_dups binary(0),
	percent_q30_bases binary(0),
	total_seqs bigint UNSIGNED,
	median_coverage float,
	contamination float,
	at_least_10x double,
	at_least_20x double,
	ploidy_estimation varchar(6),
	snp binary(0),
	indel int UNSIGNED,
	ts_tv float,
	yield bigint UNSIGNED
);
CREATE INDEX id_repository_idx
ON staging_ont_sec(id_repository);

CREATE TABLE staging_seq (
  id_repository varchar(32), 
  id_library varchar(32), 
  sequencer varchar(8), 
  date_primary datetime, 
  sum_of_total_passed_bases mediumtext, 
  sum_of_bam_size mediumtext, 
  id_index double
);
CREATE INDEX id_repository_idx
ON staging_seq(id_repository);


CREATE TABLE staging_simbiox_biosamples_patients (
  id_patient varchar(128), 
  code_repository varchar(32), 
  id_mpi longtext, 
  id_subject longtext, 
  biobank_nama varchar(255)
);
CREATE INDEX id_patient_idx
ON staging_simbiox_biosamples_patients(id_patient);
CREATE INDEX code_repository_idx
ON staging_simbiox_biosamples_patients(code_repository);


CREATE TABLE staging_pgx_report_status (
  `file_name` VARCHAR(64) PRIMARY KEY comment 'ID (PK) Uniqueness from PGx samplesheets',
  bam TEXT,
  input_creation_date DATETIME,
  id_repository VARCHAR(32),
  hub_name TEXT,
  run_name varchar(128),
  report_path_ind TEXT,
  ind_report_creation_date DATETIME,
  report_path_eng TEXT,
  eng_report_creation_date DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_repository_idx
ON staging_pgx_report_status(id_repository);


CREATE TABLE simbiox_master_status_proses (
	id TINYINT PRIMARY KEY ,
	status_proses_name varchar(50),
	status_proses_color varchar(50),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);


CREATE TABLE simbiox_master_status_transfer (
	id TINYINT PRIMARY KEY ,
	status_transfer_name varchar(50) NULL,
	status_transfer_color varchar(50) NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);


CREATE TABLE simbiox_transfer_formulir (
	id varchar(50) PRIMARY KEY,
	id_biobank varchar(50),
	id_biobank_tujuan varchar(50),
	nomor_formulir varchar(50),
	tanggal_formulir date,
	tanggal_pengiriman date,
	tanggal_penerimaan date,
	waktu_pengiriman time,
	waktu_penerimaan time,
	suhu_pengiriman varchar(10),
	suhu_penerimaan varchar(10),
	petugas_pengirim varchar(100),
	petugas_penerima varchar(100),
	jasa_ekspedisi varchar(100),
	no_resi varchar(50),
	keterangan varchar(255),
	keterangan_receive varchar(255),
	transfer_status TINYINT DEFAULT 0,
	receive_status TINYINT DEFAULT 0,
	position_start varchar(20),
	user_dm varchar(50),
	user_dm_receive varchar(50),
	user_teknisi varchar(50),
	user_teknisi_receive varchar(50),
	created_time timestamp COMMENT 'created_at from the original table',
	dm_updated_at timestamp,
	dm_receive_updated_at timestamp,
	teknisi_updated_at timestamp,
	teknisi_receive_updated_at timestamp,
	tujuan_is_biobank TINYINT DEFAULT 1,
	non_biobank_nama varchar(255),
	triple_packaging varchar(255),
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);

CREATE TABLE simbiox_transfer (
	transfer_id varchar(50) PRIMARY KEY,
	tanggal_pengiriman date,
	kode_posisi varchar(255),
	suhu_pengiriman TINYINT,
	petugas_pengirim varchar(255),
	jasa_ekspedisi varchar(255),
	petugas_penerima varchar(255),
	suhu_penerimaan TINYINT,
	tanggal_diterima date,
	id_biobank varchar(50),
	id_biobank_tujuan varchar(50),
	biosample_id varchar(50),
	repository_code varchar(50),
	transfer_formulir_id varchar(50),
	proses_status TINYINT DEFAULT 0,
	transfer_status TINYINT DEFAULT 0,
	proses_receive_status TINYINT DEFAULT 0,
	new_biosample_id varchar(50),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX biosample_id_idx
ON simbiox_transfer(biosample_id);


CREATE TABLE simbiox_log_visit_biospc (
	id varchar(50) PRIMARY KEY,
	code_origin varchar(10),
	px_id varchar(50),
	diagnose text,
	kode_icd varchar(7),
	datevisit date,
	`status` VARCHAR(5),
	`pi` varchar(50),
	code_project varchar(50),
	create_time timestamp,
	create_by varchar(255),
	id_biobank varchar(50),
	kode_kedatangan varchar(2) DEFAULT '01',
	position_start varchar(255),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_biobank_idx
ON simbiox_log_visit_biospc(id_biobank);
CREATE INDEX px_id_idx
ON simbiox_log_visit_biospc(px_id);


CREATE TABLE staging_fix_ski_id_repo (
	SELECT
		code_repository,
		origin_code_repository,
		CONCAT(REGEXP_SUBSTR(origin_code_repository, "SKI"), "_", REGEXP_SUBSTR(origin_code_repository, "\\d+")) new_origin_code_repository
	FROM
		simbiox_biosamples
	WHERE
		origin_code_repository LIKE "%SKI%"
);

CREATE INDEX new_origin_code_repository
ON staging_fix_ski_id_repo(new_origin_code_repository);


CREATE TABLE ztronpro_samples(
  starting_sample TEXT,
  original_sample_id VARCHAR(128) PRIMARY KEY comment 'Run name containing id_repository',
  original_sample_name VARCHAR(128),
  if_urgent VARCHAR(5), 
  product_name VARCHAR(32),
  `order` VARCHAR(32), 
  project_code VARCHAR(128),
  project_name VARCHAR(128),
  sample_progress VARCHAR(32),
  total_routes VARCHAR(3),
  workflow TEXT,
  start_plate_code VARCHAR(32),
  start_well VARCHAR(8),
  id_library VARCHAR(8),
  barcode VARCHAR(32),
  dnb_id VARCHAR(32),
  flow_cell_id VARCHAR(32),
  start_time DATETIME,
  completion_time DATETIME,
  batch_number VARCHAR(32),
  contract_number VARCHAR(32),
  `zone` VARCHAR(32),
  creator TEXT,
  creation_time DATETIME,
  last_modified_by TEXT,
  last_modified_time DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX starting_sample_idx
ON ztronpro_samples(starting_sample);
CREATE INDEX id_library_idx
ON ztronpro_samples(id_library);

CREATE TABLE ztronpro_analysis(
  id_repository VARCHAR(32) comment 'Code of the repository',
  run_name VARCHAR(255),
  cram TEXT,
  cram_size BIGINT UNSIGNED,
  vcf TEXT,
  vcf_size BIGINT UNSIGNED,
  date_secondary DATETIME,
  pipeline_name VARCHAR(255),
  pipeline_type VARCHAR(255),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  PRIMARY KEY (id_repository, run_name)
);
CREATE INDEX id_repository_idx
ON ztronpro_analysis(id_repository);
CREATE INDEX date_secondary_idx
ON ztronpro_analysis(date_secondary);
CREATE INDEX run_name_idx
ON ztronpro_analysis(run_name);

-- Already check for ztronpro_qc id_repository should be unique
CREATE TABLE ztronpro_qc(
  id_repository VARCHAR(32) comment 'Code of the repository',
  run_name VARCHAR(255),
  ploidy_estimation VARCHAR(6),
  percent_dups FLOAT,
  percent_gc FLOAT,
  percent_mapped FLOAT,
  percent_proper_pairs FLOAT,
  at_least_30x FLOAT,
  at_least_20x FLOAT,
  at_least_10x FLOAT,
  depth FLOAT,
  vars INT UNSIGNED,
  snp FLOAT,
  indel DOUBLE,
  ts_tv FLOAT,
  reads_raw INT UNSIGNED,
  reads_clean INT UNSIGNED,
  rate_clean FLOAT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  PRIMARY KEY (id_repository, run_name)
);
CREATE INDEX id_repository_idx
ON ztronpro_qc(id_repository);
CREATE INDEX run_name_idx
ON ztronpro_qc(run_name);


CREATE TABLE phenovar_variables(
    id VARCHAR(36) PRIMARY KEY,
    `name` TEXT,
    helper_text TEXT,
    group_id VARCHAR(36),
    group_name TEXT,
    section_id VARCHAR(36),
    section_name TEXT,
    category_id VARCHAR(36),
    category_name TEXT,
    freq_ask_update TEXT,
    master_value_id VARCHAR(36), 
    is_spesific_diseases VARCHAR(5), 
    diseases_id TEXT, 
    is_spesific_institution VARCHAR(5), 
    institution_id TEXT,
    is_mandatory VARCHAR(5), 
    have_formula VARCHAR(5),
    formula TEXT,
    types VARCHAR(5),
    main_variable VARCHAR(36),
    is_limit VARCHAR(5), 
    `limit` TEXT,
    is_continue VARCHAR(5), 
    continue_values TEXT,
    is_gender VARCHAR(5), 
    gender_value TEXT,
    is_age VARCHAR(5), 
    age_values TEXT,
    created_by VARCHAR(36),
    updated_by VARCHAR(36),
    created_at DATETIME,
    updated_at DATETIME
);
CREATE INDEX group_id_idx
ON phenovar_variables(group_id);
CREATE INDEX section_id_idx
ON phenovar_variables(section_id);
CREATE INDEX category_id_idx
ON phenovar_variables(category_id);


CREATE TABLE phenovar_categories(
    id VARCHAR(36) PRIMARY KEY,
    `name` TEXT,
    group_id VARCHAR(36),
    group_name TEXT,
    section_id VARCHAR(36),
    section_name TEXT,
    `status` VARCHAR(5),
    created_by VARCHAR(36),
    updated_by VARCHAR(36),
    created_at DATETIME,
    updated_at DATETIME
);
CREATE INDEX group_id_idx
ON phenovar_categories(group_id);
CREATE INDEX section_id_idx
ON phenovar_categories(section_id);

CREATE TABLE phenovar_documents(
  participant_id VARCHAR(10),
  institution_id VARCHAR(36),
  institution_name TEXT,
  document_type VARCHAR(255),
  question_answer JSON,
  user JSON,
  `version` VARCHAR(255),
  created_by TEXT,
  created_at DATETIME,
  PRIMARY KEY (participant_id, institution_id, document_type)
);
CREATE INDEX participant_id_idx
ON phenovar_documents(participant_id);

CREATE TABLE regina_documents(
  id_subject VARCHAR(50) PRIMARY KEY,
  composition_id VARCHAR(255),
  composition_name VARCHAR(255),
  entry_name TEXT,
  entry_content JSON,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
)
CREATE INDEX composition_name_idx
ON regina_documents(composition_name);