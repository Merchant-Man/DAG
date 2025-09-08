/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose	:  	This query is intended to be used as a report for the distiribution of top genes in PGx reports along with demographic data.
-- Author	:	Abdullah Faqih
-- Created	:  	2025-09-08
-- Changes	:
---------------------------------------------------------------------------------------------------------------------------------
*/
-- Your SQL code goes here DROP TABLE IF EXISTS gold_pgx_top_genes_demog_report; 
DROP TABLE IF EXISTS gold_pgx_top_genes_demog_report;

CREATE TABLE gold_pgx_top_genes_demog_report (
	WITH
		pgx_detail AS (
			/* NAT2 (your original block) */
			SELECT DISTINCT
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
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				COALESCE(t3.drug_name, "Isoniazid") drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "NAT2"
						ELSE t3.gene_symbol
					END,
					' - ',
					COALESCE(t3.drug_name, "Isoniazid")
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							gene_symbol = "NAT2"
							AND drug_classification = "Antituberculosis"
							AND drug_name = "Isoniazid"
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Amikacin × MTRNR1 (your example block) */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "MTRNR1 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "MTRNR1"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				COALESCE(t3.drug_name, "Amikacin") drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "MTRNR1"
						ELSE t3.gene_symbol
					END,
					' - ',
					COALESCE(t3.drug_name, "Amikacin")
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Amikacin'
							AND gene_symbol = 'MTRNR1'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Amitriptyline × CYP2D6 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2D6 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2D6"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Amitriptyline" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2D6"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Amitriptyline"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Amitriptyline || CYP2D6'
							AND gene_symbol = 'CYP2D6'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Amitriptyline × CYP2C19 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2C19 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2C19"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Amitriptyline" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2C19"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Amitriptyline"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Amitriptyline || CYP2C19'
							AND gene_symbol = 'CYP2C19'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Azathioprine × NUDT15 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "NUDT15 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "NUDT15"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Azathioprine" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "NUDT15"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Azathioprine"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Azathioprine || NUDT15'
							AND gene_symbol = 'NUDT15'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Azathioprine × TPMT */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "TPMT Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "TPMT"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Azathioprine" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "TPMT"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Azathioprine"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = "Azathioprine || TPMT"
							AND gene_symbol = 'TPMT'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Capecitabine × DPYD */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "DPYD Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "DPYD"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Capecitabine" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "DPYD"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Capecitabine"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Capecitabine'
							AND gene_symbol = 'DPYD'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Celecoxib × CYP2C9 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2C9 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2C9"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Celecoxib" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2C9"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Celecoxib"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Celecoxib'
							AND gene_symbol = 'CYP2C9'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Clopidogrel × CYP2C19 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2C19 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2C19"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Clopidogrel" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2C19"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Clopidogrel"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Clopidogrel'
							AND gene_symbol = 'CYP2C19'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Codeine × CYP2D6 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2D6 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2D6"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Codeine" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2D6"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Codeine"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Codeine || CYP2D6'
							AND gene_symbol = 'CYP2D6'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Dapsone × G6PD */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "G6PD Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "G6PD"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Dapsone" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "G6PD"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Dapsone"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Dapsone'
							AND gene_symbol = 'G6PD'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Desflurane × CACNA1S;RYR1 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CACNA1S;RYR1 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CACNA1S;RYR1"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Desflurane" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CACNA1S;RYR1"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Desflurane"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Desflurane'
							AND gene_symbol = 'CACNA1S;RYR1'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Efavirenz × CYP2B6 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2B6 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2B6"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Efavirenz" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2B6"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Efavirenz"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Efavirenz'
							AND gene_symbol = 'CYP2B6'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Fluorouracil × DPYD */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "DPYD Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "DPYD"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Fluorouracil" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "DPYD"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Fluorouracil"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Fluorouracil'
							AND gene_symbol = 'DPYD'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Gentamicin × MTRNR1 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "MTRNR1 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "MTRNR1"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Gentamicin" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "MTRNR1"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Gentamicin"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Gentamicin'
							AND gene_symbol = 'MTRNR1'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Irinotecan × UGT1A1 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "UGT1A1 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "UGT1A1"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Irinotecan" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "UGT1A1"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Irinotecan"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = "Irinotecan"
							AND gene_symbol = 'UGT1A1'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Isoflurane × CACNA1S;RYR1 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CACNA1S;RYR1 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CACNA1S;RYR1"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Isoflurane" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CACNA1S;RYR1"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Isoflurane"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Isoflurane'
							AND gene_symbol = 'CACNA1S;RYR1'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Mercaptopurine × NUDT15 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "NUDT15 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "NUDT15"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Mercaptopurine" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "NUDT15"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Mercaptopurine"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Mercaptopurine || NUDT15'
							AND gene_symbol = 'NUDT15'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Mercaptopurine × TPMT */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "TPMT Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "TPMT"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Mercaptopurine" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "TPMT"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Mercaptopurine"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Mercaptopurine || TPMT'
							AND gene_symbol = 'TPMT'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Phenytoin × CYP2C19 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2C19 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2C19"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Phenytoin" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2C19"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Phenytoin"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Phenytoin || CYP2C9'
							AND gene_symbol = 'CYP2C19'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Phenytoin × CYP2C9 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2C9 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2C9"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Phenytoin" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2C9"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Phenytoin"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Phenytoin || CYP2C19'
							AND gene_symbol = 'CYP2C9'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Primaquine × G6PD */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "G6PD Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "G6PD"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Primaquine" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "G6PD"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Primaquine"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Primaquine'
							AND gene_symbol = 'G6PD'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Rosuvastatin × SLCO1B1 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "SLCO1B1 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "SLCO1B1"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Rosuvastatin" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "SLCO1B1"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Rosuvastatin"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Rosuvastatin || SLCO1B1'
							AND gene_symbol = 'SLCO1B1'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Rosuvastatin × ABCG2 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "ABCG2 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "ABCG2"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Rosuvastatin" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "ABCG2"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Rosuvastatin"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Rosuvastatin || ABCG2'
							AND gene_symbol = 'ABCG2'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Sevoflurane × CACNA1S;RYR1 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CACNA1S;RYR1 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CACNA1S;RYR1"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Sevoflurane" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CACNA1S;RYR1"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Sevoflurane"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Sevoflurane'
							AND gene_symbol = 'CACNA1S;RYR1'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Streptomycin × MTRNR1 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "MTRNR1 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "MTRNR1"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Streptomycin" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "MTRNR1"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Streptomycin"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Streptomycin'
							AND gene_symbol = 'MTRNR1'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Tamoxifen × CYP2D6 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2D6 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2D6"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Tamoxifen" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2D6"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Tamoxifen"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Tamoxifen'
							AND gene_symbol = 'CYP2D6'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Tramadol × CYP2D6 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2D6 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2D6"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Tramadol" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2D6"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Tramadol"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Tramadol || CYP2D6'
							AND gene_symbol = 'CYP2D6'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Voriconazole × CYP2C19 */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "CYP2C19 Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "CYP2C19"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Voriconazole" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "CYP2C19"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Voriconazole"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Voriconazole || CYP2C19'
							AND gene_symbol = 'CYP2C19'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
			UNION ALL
			/* Warfarin × VKORC1;CYP2C9;CY  (kept exactly as you wrote) */
			SELECT DISTINCT
				t1.id_subject,
				t2.province,
				t4.iso_name,
				CASE
					WHEN t3.phenotype_text IS NULL THEN "VKORC1;CYP2C9;CY Tidak Dapat Ditentukan"
					ELSE t3.phenotype_text
				END phenotype_text,
				CASE
					WHEN t3.gene_symbol IS NULL THEN "VKORC1;CYP2C9;CY"
					ELSE t3.gene_symbol
				END gene_symbol,
				CASE
					WHEN rec_category = 1 THEN "Change Prescription"
					WHEN rec_category = 2 THEN "Increase Starting Dose"
					WHEN rec_category = 3 THEN "Decrease Starting Dose"
					WHEN rec_category = 4 THEN "Increase Monitoring"
					WHEN rec_category = 5 THEN "Follow Standard Dosing"
				END recommendation,
				"Warfarin" drug_name,
				CONCAT(
					CASE
						WHEN t3.gene_symbol IS NULL THEN "VKORC1;CYP2C9;CY"
						ELSE t3.gene_symbol
					END,
					' - ',
					"Warfarin"
				) AS gene_drug
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
									ind_report_creation_date IS NOT NULL
									OR eng_report_creation_date IS NOT NULL
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
						rec_category,
						drug_name
					FROM
						dwh_restricted.gold_pgx_detail_report
					WHERE
						(
							drug_name = 'Warfarin'
							AND gene_symbol = 'VKORC1;CYP2C9;CY'
							AND phenotype_text IS NOT NULL
						)
				) t3 ON t1.file_name = t3.order_id
				LEFT JOIN provinsi t4 ON t2.province = UPPER(t4.`name`)
		)
	SELECT
		t1.*,
		t2.hospital_name
	FROM
		pgx_detail t1
		LEFT JOIN (
			SELECT DISTINCT
				id_subject,
				hospital_name
			FROM
				staging_demography
		) t2 ON t1.id_subject = t2.id_subject
);

ALTER TABLE gold_pgx_top_genes_demog_report
MODIFY COLUMN province VARCHAR(128);

ALTER TABLE gold_pgx_top_genes_demog_report
MODIFY COLUMN phenotype_text VARCHAR(128);

ALTER TABLE gold_pgx_top_genes_demog_report
MODIFY COLUMN recommendation VARCHAR(128);

ALTER TABLE gold_pgx_top_genes_demog_report
MODIFY COLUMN id_subject VARCHAR(36);

ALTER TABLE gold_pgx_top_genes_demog_report
MODIFY COLUMN drug_name VARCHAR(128);

ALTER TABLE gold_pgx_top_genes_demog_report
MODIFY COLUMN gene_drug VARCHAR(128);

ALTER TABLE gold_pgx_top_genes_demog_report
MODIFY COLUMN hospital_name VARCHAR(128);

CREATE INDEX province_idx ON gold_pgx_top_genes_demog_report (province);

CREATE INDEX id_subject_idx ON gold_pgx_top_genes_demog_report (id_subject);

CREATE INDEX phenotype_text_idx ON gold_pgx_top_genes_demog_report (phenotype_text);

CREATE INDEX recommendation_idx ON gold_pgx_top_genes_demog_report (recommendation);

CREATE INDEX drug_name_idx ON gold_pgx_top_genes_demog_report (drug_name);

CREATE INDEX gene_drug_idx ON gold_pgx_top_genes_demog_report (gene_drug);

CREATE INDEX hospital_name_idx ON gold_pgx_top_genes_demog_report (hospital_name);