
-- create
CREATE TABLE regina_demography (
  id_subject VARCHAR(50) PRIMARY KEY comment 'Encrypted subject ID from RegINA',
  age TINYINT UNSIGNED NOT NULL,
  sex VARCHAR(6) NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP comment 'Timestamp of record creation. Using MySQL TZ.',
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last update. Using MySQL TZ.',
  creationDate DATETIME NOT NULL comment 'Timestamp of record creation from PhenoVar',
);

-- create
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
specimen_type_repo VARCHAR(8)
);


CREATE TABLE master_sample_type (
id SMALLINT UNSIGNED PRIMARY KEY,
sample_name	VARCHAR(32),
active	BOOLEAN, 
id_biobank VARCHAR(128),
);


CREATE TABLE master_status (
id SMALLINT UNSIGNED PRIMARY KEY,
status_name	VARCHAR(32),
status_color	VARCHAR(32)
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
biobank_repo VARCHAR(8)
);


CREATE TABLE simbiox_biosamples (
id_patient VARCHAR(128) PRIMARY KEY  comment'ID of the record from biosample symbiox data'
id_patient	VARCHAR(128) NOT NULL comment 'Patient ID of biosample'
code_repository VARCHAR(32) NOT NULL comment 'Code repository of biosample'
code_box VARCHAR(16) NOT NULL comment 'Box code of the biosample'
code_position VARCHAR(16) NOT NULL commnet 'Position code of the biosample'
date_received DATETIME 
date_enumerated
origin_biobank
origin_code_repository
origin_code_box
biosample_type
biosample_specimen
biosample_volume
biosample_status

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



id_patient	VARCHAR(128) NOT NULL comment 'Patient ID of biosample'
code_repository VARCHAR(32) NOT NULL comment 'Code repository of biosample'
code_box VARCHAR(16) NOT NULL comment 'Box code of the biosample'
code_position VARCHAR(16) NOT NULL commnet 'Position code of the biosample'
date_received DATETIME 
date_enumerated
origin_biobank
origin_code_repository
origin_code_box
biosample_type
biosample_specimen
biosample_volume
biosample_status

);

id comment 'ID of the record from biosample symbiox data '
id_patient	
code_repository
code_box
code_position
date_received
date_enumerated
origin_biobank
origin_code_repository
origin_code_box
biosample_type
biosample_specimen
biosample_volume
biosample_status
2		text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
3		text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
4	code_position	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
5	date_received	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
6	date_enumerated	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
7	origin_biobank	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
8	origin_code_repository	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
9	origin_code_box	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
10	biosample_type	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
11	biosample_specimen	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			
12	biosample_volume	bigint	NULL	NULL	YES	NULL			
13	biosample_status	text	utf8mb4	utf8mb4_0900_ai_ci	YES	NULL			

CREATE TABLE simbiox_biosamples_latest (

)

-- insert
INSERT INTO regina_demography (id_subject, age, sex) VALUES ('a066ba03-8d82-4eca-b628-d3f712b049de', 74, 'Female');
INSERT INTO regina_demography (id_subject, age, sex) VALUES ('1d3d4203-9275-4f5b-9de0-72edb12fe091', 68, 'Male');

-- fetch 
SELECT * FROM regina_demography;

DO SLEEP(3);

-- update
UPDATE 
  regina_demography
SET age = 75, sex = 'Male'
WHERE id_subject = 'a066ba03-8d82-4eca-b628-d3f712b049de';



-- fetch 
SELECT * FROM regina_demography;




INSERT INTO regina_demography (id_subject, age, sex)
ON DUPLICATE KEY UPDATE age = <>, sex = <>


-- query = """INSERT INTO table (id, name, age) VALUES(%s, %s, %s)
-- ON DUPLICATE KEY UPDATE name=%s, age=%s"""
-- engine.execute(query, (df.id[i], df.name[i], df.age[i], df.name[i], df.age[i]))