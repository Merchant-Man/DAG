/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as simbiox transfer report
-- Author   :   Abdullah Faqih
-- Created  :   14-05-2025
-- Changes	:   16-05-2025 Adding origin_code_repository
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here 
DROP TABLE IF EXISTS gold_simbiox_transfer;

CREATE TABLE gold_simbiox_transfer (
WITH
	log_visit AS (
		SELECT
			px_id,
			patient_categ
		FROM
			(
				SELECT
					px_id,
					CASE
						WHEN kode_icd LIKE "z00%" THEN "Control"
						ELSE "Patient"
					END patient_categ,
					ROW_NUMBER() OVER (
						PARTITION BY
							px_id
						ORDER BY
							create_time DESC
					) rn
				FROM
					simbiox_log_visit_biospc
			) t
		WHERE
			rn = 1
	)
SELECT
	t1.*,
	t2.code_repository,
    t2.origin_code_repository,
	t3.id_subject,
	t4.biobank_nama patient_biobank,
	t5.patient_categ,
	COALESCE(rd.sex, pp.sex) registry_sex,
	YEAR(tanggal_formulir) AS year_formulir,
	YEAR(tanggal_pengiriman) AS year_pengiriman,
	YEAR(tanggal_penerimaan) AS year_penerimaan,
	MONTHNAME(tanggal_formulir) AS month_formulir,
	MONTHNAME(tanggal_pengiriman) AS month_pengiriman,
	MONTHNAME(tanggal_penerimaan) AS month_penerimaan
FROM
	staging_simbiox_transfer t1
	LEFT JOIN simbiox_biosamples t2 ON t1.biosample_id = t2.id
	LEFT JOIN simbiox_patients t3 ON t2.id_patient = t3.id_patient
	LEFT JOIN master_biobank t4 ON t3.id_biobank = t4.id_biobank
	LEFT JOIN log_visit t5 ON t3.id_patient = t5.px_id
	LEFT JOIN regina_demography rd ON t3.id_subject = rd.id_subject
	LEFT JOIN phenovar_participants pp ON t3.id_subject = pp.id_subject
);

CREATE INDEX idx_gold_simbiox_transfer_nomor_formulir ON gold_simbiox_transfer (nomor_formulir);
CREATE INDEX idx_gold_simbiox_transfer_biobank_asal ON gold_simbiox_transfer (biobank_asal);
CREATE INDEX idx_gold_simbiox_transfer_biobank_tujuan ON gold_simbiox_transfer (biobank_tujuan);
CREATE INDEX idx_gold_simbiox_transfer_tanggal_pengiriman ON gold_simbiox_transfer (tanggal_pengiriman);
CREATE INDEX idx_gold_simbiox_transfer_tanggal_penerimaan ON gold_simbiox_transfer (tanggal_penerimaan);
CREATE INDEX idx_gold_simbiox_transfer_code_repository ON gold_simbiox_transfer (code_repository);
CREATE INDEX idx_gold_simbiox_transfer_id_subject ON gold_simbiox_transfer (id_subject);
CREATE INDEX idx_gold_simbiox_transfer_patient_categ ON gold_simbiox_transfer (patient_categ);
CREATE INDEX idx_year_formulir ON gold_simbiox_transfer (year_formulir);
CREATE INDEX idx_year_pengiriman ON gold_simbiox_transfer (year_pengiriman);
CREATE INDEX idx_year_penerimaan ON gold_simbiox_transfer (year_penerimaan);
CREATE INDEX idx_month_formulir ON gold_simbiox_transfer (month_formulir);
CREATE INDEX idx_month_pengiriman ON gold_simbiox_transfer (month_pengiriman);
CREATE INDEX idx_month_penerimaan ON gold_simbiox_transfer (month_penerimaan);