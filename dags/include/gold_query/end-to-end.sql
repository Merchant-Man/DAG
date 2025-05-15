WITH
	simbiox_data AS (
		SELECT
			t2.id_subject,
			t7.patient_categ,
			t3.biobank_nama,
			code_repository,
			t5.specimen_type_name,
			CASE
				WHEN specimen_type_name = "Buffycoat" THEN 1
				ELSE 0
			END is_buffycoat,
			CASE
				WHEN specimen_type_name = "Plasma EDTA" THEN 1
				ELSE 0
			END is_plasma,
			CASE
				WHEN specimen_type_name = "Urine" THEN 1
				ELSE 0
			END is_urin,
			CASE
				WHEN specimen_type_name = "DNA" THEN 1
				ELSE 0
			END is_dna,
			CASE
				WHEN specimen_type_name = "Whole Blood" THEN 1
				ELSE 0
			END is_whole_blood,
			CASE
				WHEN specimen_type_name = "Serum" THEN 1
				ELSE 0
			END is_serum,
			CASE
				WHEN specimen_type_name = "Sputum" THEN 1
				ELSE 0
			END is_sputum,
			CASE
				WHEN specimen_type_name = "Isolat" THEN 1
				ELSE 0
			END is_isolat,
			CASE
				WHEN specimen_type_name = "Air Mata" THEN 1
				ELSE 0
			END is_airmata,
			CASE
				WHEN specimen_type_name = "Jarigan" THEN 1
				ELSE 0
			END is_jaringan,
			t6.sample_name,
			t4.status_name,
			DATE(t1.date_enumerated) AS input_date,
			DATE(t1.date_received) receive_date
		FROM
			simbiox_biosamples t1
			LEFT JOIN simbiox_patients t2 ON t1.id_patient = t2.id_patient
			LEFT JOIN (
				SELECT DISTINCT
					px_id,
					CASE
						WHEN kode_icd LIKE "z00%" THEN "Control"
						ELSE "Patient"
					END patient_categ
				FROM
					simbiox_log_visit_biospc
			) t7 ON t7.px_id = t2.id_patient
			LEFT JOIN master_biobank t3 ON t1.id_biobank = t3.id_biobank
			LEFT JOIN master_status t4 ON t1.biosample_status = t4.id
			LEFT JOIN master_specimen t5 ON t1.biosample_specimen = t5.id
			LEFT JOIN master_sample_type t6 ON t1.biosample_type = t6.id
	),
	agg_simbiox_data AS (
		SELECT
			id_subject,
			patient_categ,
			code_repository,
			CASE
				WHEN sum_buffycoat > 0 THEN 1
				ELSE 0
			END is_buffycoat,
			sum_buffycoat,
			CASE
				WHEN sum_plasma > 0 THEN 1
				ELSE 0
			END is_plasma,
			sum_plasma,
			CASE
				WHEN sum_urin > 0 THEN 1
				ELSE 0
			END is_urin,
			sum_urin,
			CASE
				WHEN sum_dna > 0 THEN 1
				ELSE 0
			END is_dna,
			sum_dna,
			CASE
				WHEN sum_whole_blood > 0 THEN 1
				ELSE 0
			END is_whole_blood,
			sum_whole_blood,
			CASE
				WHEN sum_serum > 0 THEN 1
				ELSE 0
			END is_serum,
			sum_serum,
			CASE
				WHEN sum_sputum > 0 THEN 1
				ELSE 0
			END is_sputum,
			sum_sputum,
			CASE
				WHEN sum_isolat > 0 THEN 1
				ELSE 0
			END is_isolat,
			sum_isolat,
			CASE
				WHEN sum_airmata > 0 THEN 1
				ELSE 0
			END is_airmata,
			sum_airmata,
			CASE
				WHEN sum_jaringan > 0 THEN 1
				ELSE 0
			END is_jaringan,
			sum_jaringan
		FROM
			(
				SELECT
					id_subject,
					patient_categ,
					GROUP_CONCAT(code_repository) code_repository,
					SUM(is_buffycoat) sum_buffycoat,
					SUM(is_plasma) sum_plasma,
					SUM(is_urin) sum_urin,
					SUM(is_dna) sum_dna,
					SUM(is_whole_blood) sum_whole_blood,
					SUM(is_serum) sum_serum,
					SUM(is_sputum) sum_sputum,
					SUM(is_isolat) sum_isolat,
					SUM(is_airmata) sum_airmata,
					SUM(is_jaringan) sum_jaringan
				FROM
					simbiox_data
				GROUP BY
					1,
					2
			) t
	),
	participants AS (
		SELECT
			id_subject,
			age_at_recruitment,
			sex,
			COALESCE(hospital_name, biobank_nama) hospital_name,
			provinsi.name nik_province,
			enrollment_date
		FROM
			(
				SELECT
					t1.id_subject,
					t1.age_at_recruitment,
					t1.sex,
					CASE
						WHEN hospital_name = "RSUP PROF. DR. IGNG NGOERAH DENPASAR" THEN "RSUP Prof. Dr. IGNG Ngoerah Denpasar"
						WHEN hospital_name = "RSUP PERSAHABATAN" THEN "RSUP Persahabatan"
						WHEN hospital_name = "RSUP DR. SARDJITO" THEN "RSUP Dr. Sardjito"
						WHEN hospital_name = "RSPI PROF. DR. SULIANTI SAROSO" THEN "RSPI Prof. Dr. Sulianti Saroso"
						WHEN hospital_name = "RS PON PROF. DR. DR. MAHAR MARDJONO" THEN "RS PON Prof. Dr. dr. Mahar Mardjono"
						WHEN hospital_name = "RS KANKER DHARMAIS" THEN "RS Kanker Dharmais"
						WHEN hospital_name = "RS DR. CIPTO MANGUNKUSUMO" THEN "RS Dr. Cipto Mangunkusumo"
					END hospital_name,
					t3.biobank_nama,
					t2.nik,
					REGEXP_SUBSTR(t2.nik, "[\\d]{2}") province_code,
					t1.demog_created_at enrollment_date
				FROM
					superset_dev.regina_demography_basic_info t1
					INNER JOIN dwh_restricted.decrypt_regina t2 ON t1.id_subject = t2.id_subject
					LEFT JOIN (
						SELECT DISTINCT
							t1.id_subject,
							CASE
								WHEN t2.biobank_nama LIKE "%RSCM%" THEN "RS Dr. Cipto Mangunkusumo"
								WHEN t2.biobank_nama LIKE "% RSPI Prof. Dr. Sulianti Saroso%" THEN "RSPI Prof. Dr. Sulianti Saroso"
								WHEN t2.biobank_nama LIKE "%RSUP Dr. Sardjito%" THEN "RSUP Dr. Sardjito"
								WHEN t2.biobank_nama LIKE "%RS PON Prof. Dr. dr. Mahar Mardjono%" THEN "RS PON Prof. Dr. dr. Mahar Mardjono"
								WHEN t2.biobank_nama LIKE "%Biobank RSUP Persahabatan%" THEN "RSUP Persahabatan"
								WHEN t2.biobank_nama LIKE "%Biobank RS Kanker Dharmais%" THEN "RS Kanker Dharmais"
								WHEN t2.biobank_nama LIKE "%Biobank RSUP Prof. Dr. IGNG Ngoerah Denpasar%" THEN "RSUP Prof. Dr. IGNG Ngoerah Denpasar"
								ELSE biobank_nama
							END biobank_nama
						FROM
							simbiox_patients t1
							LEFT JOIN master_biobank t2 ON t1.id_biobank = t2.id_biobank
						WHERE
							id_subject != ""
							AND id_subject IS NOT NULL
							AND id_subject != "-"
					) t3 ON t1.id_subject = t3.id_subject
			) t
			LEFT JOIN provinsi ON t.province_code = provinsi.id
		WHERE
			COALESCE(hospital_name, biobank_nama) IS NOT NULL
		UNION ALL
		SELECT
			t1.id_subject,
			TIMESTAMPDIFF(YEAR, t2.birth_date, DATE(t1.creation_date)) age_at_recruitment,
			t1.sex,
			t1.hospital_name,
			t3.name nik_province,
			t1.creation_date enrollment_date
		FROM
			superset_dev.phenovar_participants t1
			INNER JOIN dwh_restricted.decrypted_phenovar_participants t2 ON t1.id_subject = t2.id_subject
			LEFT JOIN provinsi t3 ON t1.province_code = t3.id
	),
	pgx_report AS (
		SELECT
			t7.id_repository,
			CASE
				WHEN ind_report_creation_date IS NULL THEN 0
				ELSE 1
			END is_pgx_created,
			t8.id_subject
		FROM
			superset_dev.staging_pgx_report_status t7
			LEFT JOIN staging_simbiox_biosamples_patients t8 ON t7.id_repository = t8.code_repository
	)
SELECT
	t4.*,
	CASE
		WHEN t5.code_repository IS NOT NULL THEN 1
		ELSE 0
	END biobank_available,
	t5.patient_categ,
	t5.is_buffycoat,
	t5.is_plasma,
	t5.is_sputum,
	t5.is_whole_blood,
	CASE
		WHEN t6.id_subject IS NULL THEN 0
		ELSE 1
	END is_secondary_analysed,
	t6.is_qc_secondary_analysed,
	t6.sequencer secondary_sequencer,
	CASE
		WHEN pgx_report.is_pgx_created IS NULL THEN 0
		ELSE 1
	END is_pgx_created
FROM
	participants t4
	LEFT JOIN agg_simbiox_data t5 ON t4.id_subject = t5.id_subject
	LEFT JOIN (
		SELECT DISTINCT
			id_subject,
			sequencer,
			CASE
				WHEN qc_category = "Pass" THEN 1
				ELSE 0
			END is_qc_secondary_analysed
		FROM
			superset_dev.gold_qc
	) t6 ON t4.id_subject = t6.id_subject
	LEFT JOIN pgx_report ON t4.id_subject = pgx_report.id_subject