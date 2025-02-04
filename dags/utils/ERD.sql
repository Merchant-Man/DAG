CREATE TABLE regina_demography (
  id_subject VARCHAR(50) PRIMARY KEY comment 'Encrypted subject ID from RegINA',
  age TINYINT UNSIGNED NOT NULL,
  sex VARCHAR(6) NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  creationDate DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar',
);


CREATE TABLE phenovar_participants (
  id_subject VARCHAR(10) PRIMARY KEY comment 'Encrypted subject ID from PhenoVar',
  encrypt_full_name VARCHAR(225) NOT NULL comment 'Encrypted full name from PhenoVar',
  encrypt_nik VARCHAR(225) NOT NULL comment 'Encrypted NIK from PhenoVar', 
  encrypt_birth_date VARCHAR(225) NOT NULL comment 'Encrypted birth date from PhenoVar',
  sex VARCHAR(6) NOT NULL,
  source VARCHAR(64) comment 'Source of data i.e. SatuSehat',
  province VARCHAR(64) NOT NULL,
  district VARCHAR(64) NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  creation_date DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar',
  updation_date DATETIME NOT NULL comment 'Timestamp of last update from PhenoVar'
);


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
	

CREATE TABLE  simbiox_biosamples(
id_patient VARCHAR(128) PRIMARY KEY  comment'ID (PK) of the record from simbiox patient data',
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
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX code_repository_idx
ON simbiox_biosamples (code_repository);


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
  `name` VARCHAR(128),
  `status` VARCHAR(32),
  tag_technical_tags text,
  tag_user_tags text,
  tag_connetor_tags text,
  tag_run_in_tags text,
  application_id VARCHAR(128),
  application_name VARCHAR(128)
);
CREATE INDEX id_library_idx
ON ica_samples (id_library);

-- Already check for illumina_qc id_repository should be unique
CREATE TABLE illumina_qc(
  id_repository VARCHAR(32) PRIMARY KEY comment 'Code of the repository',
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
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_repository_idx
ON illumina_qc(id_repository);

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
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.'
);
CREATE INDEX id_repository_idx
ON mgi_qc(id_repository);
ALTER TABLE mgi_qc
ADD CONSTRAINT PK_qs PRIMARY KEY (id_repository, ploidy_estimation, vars, snp, indel, depth, percent_dups, median_coverage)


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