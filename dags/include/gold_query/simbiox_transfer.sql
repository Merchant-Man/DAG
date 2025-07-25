/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as simbiox transfer report
-- Author   :   Abdullah Faqih
-- Created  :   14-05-2025
-- Changes	:   16-05-2025 Adding origin_code_repository
16-06-2025 Adding backdoor for RSJPDHK/2024/01 form
25-06-2025 Excluding the Biobank Pusat from the transfer report
30-06-2025 Adding pattern to prevent unlucky read access
---------------------------------------------------------------------------------------------------------------------------------
*/
-- Your SQL code goes here 
DROP TABLE IF EXISTS gold_simbiox_transfer_new;

CREATE TABLE gold_simbiox_transfer_new (
	WITH

	all_demog AS (
		SELECT
			*,
			CASE
				WHEN FLOOR(DATEDIFF(creation_date, birth_date) / 365.25) < 0 THEN 0
				ELSE FLOOR(DATEDIFF(creation_date, birth_date) / 365.25)
			END age_at_recruitment
		FROM
			(
				SELECT
					id_subject,
					birthdate birth_date,
					CASE
						WHEN gender = 'Perempuan' THEN 'FEMALE'
						WHEN gender = 'Laki-laki' THEN 'MALE'
					END sex,
					creationDate creation_date
				FROM
					dwh_restricted.decrypt_regina
				UNION ALL
				SELECT
					id_subject,
					CASE
						WHEN birth_date = "0976-05-19" THEN DATE("1976-05-19")
						ELSE birth_date
					END birth_date,
					UPPER(sex) sex,
					created_at
				FROM
					dwh_restricted.decrypted_phenovar_participants
			) t
	),
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
		),
		temp AS (
			SELECT
				t1.*,
				t2.code_repository,
				t2.origin_code_repository,
				t3.id_subject,
				t4.biobank_nama patient_biobank,
				t5.patient_categ,
				demog.sex registry_sex,
				demog.age_at_recruitment,
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
				LEFT JOIN all_demog demog ON t3.id_subject = demog.id_subject
			WHERE
				t1.biobank_asal != 'Biobank Pusat'
		),
		dupl_to_seq_temp AS (
			SELECT
				*,
				DENSE_RANK() OVER (
					PARTITION BY
						origin_code_repository
					ORDER BY
						tanggal_formulir
				) dup_orig_code_repo_rn
			FROM
				temp
			WHERE
				is_to_central_seq = 1
				AND status_transfer_name = "Dikirim"
		)
	SELECT
		t1.*,
		CASE
			WHEN t2.dup_orig_code_repo_rn > 1 THEN NULL
			ELSE t2.origin_code_repository
		END origin_code_repository_non_dup
	FROM
		temp t1
		LEFT JOIN dupl_to_seq_temp t2 ON t1.biosample_id = t2.biosample_id
		AND t1.id = t2.id
	UNION ALL
	SELECT
		t1.id biosample_id,
		"backdoor_form_id_1" id,
		"Backdoor_toCentral_SKI/2025/01" nomor_formulir,
		DATE("2025-05-19") tanggal_formulir,
		"Biobank Sentral (BB Binomika)" biobank_asal,
		"Biobank Sentral (BB Binomika)" biobank_tujuan,
		DATE("2025-05-19") tanggal_pengiriman,
		NULL waktu_pengiriman,
		NULL suhu_pengiriman,
		"OK" keterangan_pengiriman,
		"Dikirim" status_pengiriman,
		"Backdoor" petugas_pengirim,
		DATE("2025-05-19") tanggal_penerimaan,
		NULL waktu_penerimaan,
		NULL suhu_penerimaan,
		"OK" keterangan_penerimaan,
		"OK" status_penerimaan,
		"Backdoor" petugas_penerima,
		"Dikirim" status_transfer_name,
		TRUE tujuan_is_biobank,
		NULL non_biobank_nama,
		NULL triple_packaging,
		FALSE is_to_central_seq,
		t1.code_repository,
		CONCAT(SUBSTR(t1.origin_code_repository, 1, 3), SUBSTR(t1.origin_code_repository, 4)) origin_code_repository,
		t2.id_subject,
		t3.biobank_nama patient_biobank,
		"Control" patient_categ,
		t4.sex registry_sex,
		t4.age_at_recruitment,
		"2025" year_formulir,
		"2025" year_pengiriman,
		"2025" year_penerimaan,
		"May" month_formulir,
		"May" month_pengiriman,
		"May" month_penerimaan,
		NULL origin_code_repository_non_dup
	FROM
		simbiox_biosamples t1
		LEFT JOIN simbiox_patients t2 ON t1.id_patient = t2.id_patient
		LEFT JOIN master_biobank t3 ON t2.id_biobank = t3.id_biobank
		LEFT JOIN all_demog t4 ON t2.id_subject = t4.id_subject
	WHERE
		t1.origin_code_repository LIKE "SKI%"
		OR t1.code_repository LIKE "SKI%"
);

# From here We will hard-coded this specific formulir until bakcend fix it! 
INSERT INTO
	gold_simbiox_transfer_new (
		SELECT
			biosample_id,
			id,
			nomor_formulir,
			tanggal_formulir,
			biobank_asal,
			'Biobank Sentral (BB Binomika)' biobank_tujuan,
			tanggal_pengiriman,
			waktu_pengiriman,
			suhu_pengiriman,
			keterangan_pengiriman,
			status_pengiriman,
			petugas_pengirim,
			DATE('2024-05-08') tanggal_penerimaan, # hard-coded
			waktu_penerimaan,
			suhu_penerimaan,
			keterangan_penerimaan,
			"OK" status_penerimaan,
			petugas_penerima,
			status_transfer_name,
			1 tujuan_is_biobank,
			non_biobank_nama,
			triple_packaging,
			is_to_central_seq,
			code_repository,
			origin_code_repository,
			id_subject,
			patient_biobank,
			patient_categ,
			registry_sex,
			age_at_recruitment,
			year_formulir,
			year_pengiriman,
			'2024' year_penerimaan,
			month_formulir,
			month_pengiriman,
			'May' month_penerimaan,
			origin_code_repository_non_dup
		FROM
			gold_simbiox_transfer_new
		WHERE
			nomor_formulir = 'RSJPDHK/2024/01'
	);

# Delete the wrong records
DELETE FROM gold_simbiox_transfer_new
WHERE
	nomor_formulir = 'RSJPDHK/2024/01'
	AND biobank_tujuan IS NULL;

CREATE INDEX idx_gold_simbiox_transfer_nomor_formulir ON gold_simbiox_transfer_new (nomor_formulir);

CREATE INDEX idx_gold_simbiox_transfer_biosample_id ON gold_simbiox_transfer_new (biosample_id);

CREATE INDEX idx_gold_simbiox_transfer_biobank_asal ON gold_simbiox_transfer_new (biobank_asal);

CREATE INDEX idx_gold_simbiox_transfer_biobank_tujuan ON gold_simbiox_transfer_new (biobank_tujuan);

CREATE INDEX idx_gold_simbiox_transfer_tanggal_pengiriman ON gold_simbiox_transfer_new (tanggal_pengiriman);

CREATE INDEX idx_gold_simbiox_transfer_tanggal_penerimaan ON gold_simbiox_transfer_new (tanggal_penerimaan);

CREATE INDEX idx_gold_simbiox_transfer_code_repository ON gold_simbiox_transfer_new (code_repository);

CREATE INDEX idx_gold_simbiox_transfer_id_subject ON gold_simbiox_transfer_new (id_subject);

CREATE INDEX idx_gold_simbiox_transfer_patient_categ ON gold_simbiox_transfer_new (patient_categ);

CREATE INDEX idx_gold_simbiox_transfer_is_to_central_seq ON gold_simbiox_transfer_new (is_to_central_seq);

CREATE INDEX idx_year_formulir ON gold_simbiox_transfer_new (year_formulir);

CREATE INDEX idx_year_pengiriman ON gold_simbiox_transfer_new (year_pengiriman);

CREATE INDEX idx_year_penerimaan ON gold_simbiox_transfer_new (year_penerimaan);

CREATE INDEX idx_month_formulir ON gold_simbiox_transfer_new (month_formulir);

CREATE INDEX idx_month_pengiriman ON gold_simbiox_transfer_new (month_pengiriman);

CREATE INDEX idx_month_penerimaan ON gold_simbiox_transfer_new (month_penerimaan);

RENAME TABLE gold_simbiox_transfer TO gold_simbiox_transfer_old,
gold_simbiox_transfer_new TO gold_simbiox_transfer;

-- 4. Drop the old table as cleanup
DROP TABLE IF EXISTS gold_simbiox_transfer_old;