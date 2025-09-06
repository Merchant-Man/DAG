/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose:  This query is intended to be used as the source of QC metrics.
-- Author:   
-- Created:  
-- Modified|by: <Date of Last Modification> 
-- Changes:  <Brief description of changes made in each modification, along with the person who modified it>
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here 
DROP TABLE IF EXISTS gold_nat2_demog_report; 

CREATE TABLE gold_nat2_demog_report (SELECT DISTINCT
	t1.id_subject,
	t2.province,
	t4.iso_name,
	CASE
		WHEN t3.phenotype_text IS NULL THEN "NAT2 Tidak Dapat Ditentukan"
		ELSE t3.phenotype_text
	END phenotype_text,
	CASE
		WHEN t3.gene_symbol IS NULL THEN "NAT2"
		ELSE t3.gene_symbol
	END gene_symbol,
	CASE
		WHEN t3.rec_category IS NULL THEN 5
		ELSE t3.rec_category
	END rec_category
FROM
	(
		SELECT
			t.*
		FROM
			(
				SELECT
					ROW_NUMBER() OVER (
						PARTITION BY
							id_subject
						ORDER BY
							ind_report_creation_date DESC
					) rn_ind,
					ROW_NUMBER() OVER (
						PARTITION BY
							id_subject
						ORDER BY
							eng_report_creation_date DESC
					) rn_eng,
					id_subject,
					file_name
				FROM
					superset_dev.gold_pgx_report
				WHERE
					(
						NOT ind_report_creation_date IS NULL
						OR NOT eng_report_creation_date IS NULL
					)
			) t
		WHERE
			t.rn_ind = 1
	) t1
	LEFT JOIN superset_dev.staging_demography t2 ON t1.id_subject = t2.id_subject
	LEFT JOIN (
		SELECT DISTINCT
			order_id,
			phenotype_text,
			gene_symbol,
			rec_category
		FROM
			dwh_restricted.gold_pgx_detail_report
		WHERE
			(
				gene_symbol = "NAT2"
				AND drug_classification = "Antituberculosis"
				AND phenotype_text IS NOT NULL
			)
	) t3 ON t1.file_name = t3.order_id
	LEFT JOIN provinsi t4
	ON t2.province = UPPER(t4.`name`));

ALTER TABLE gold_nat2_demog_report MODIFY COLUMN province VARCHAR(128);
ALTER TABLE gold_nat2_demog_report MODIFY COLUMN phenotype_text VARCHAR(128);
ALTER TABLE gold_nat2_demog_report MODIFY COLUMN id_subject VARCHAR(36);

CREATE INDEX province_idx ON gold_nat2_demog_report (province); 
CREATE INDEX id_subject_idx ON gold_nat2_demog_report (id_subject); 
CREATE INDEX phenotype_text_idx ON gold_nat2_demog_report (phenotype_text); 