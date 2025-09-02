START TRANSACTION;

DELETE FROM staging_demography;

INSERT INTO
	staging_demography (
		WITH
			regina_ec AS (
				SELECT
					id_subject,
					GROUP_CONCAT(DISTINCT nomor_ec SEPARATOR ": ") nomor_ec,
					GROUP_CONCAT(DISTINCT judul_penelitian SEPARATOR ": ") judul_penelitian,
					GROUP_CONCAT(DISTINCT nama_pi SEPARATOR ": ") nama_pi,
					COUNT(DISTINCT nomor_ec) ct_ec
				FROM
					(
						SELECT
							id_subject,
							CASE
								WHEN nomor_ethical_clearance LIKE "%VII.14/LT/2024" THEN "0512/UN14.2.2.VII.14/LT/2024"
								WHEN nomor_ethical_clearance LIKE "%VII.14/LT/2022" THEN "2774/UN14.2.2.VII.14/LT/2022"
								WHEN nomor_ethical_clearance LIKE "%KEPK/XII/2022%" THEN "286/KEPK/XII/2022"
								WHEN nomor_ethical_clearance LIKE "%KE/FK/0017/EC/2023" THEN "KE/FK/0017/EC/2023"
								WHEN nomor_ethical_clearance LIKE "KET-1203/UN2.F1/ETIK/PPM.00.02%" THEN "KET-1203/UN2.F1/ETIK/PPM.00.02/2023"
								WHEN nomor_ethical_clearance LIKE "%KET-42/UN2.F1/ETIK/PPM.00.02/2023%" THEN "KET-42/UN2.F1/ETIK/PPM.00.02/2023"
								WHEN nomor_ethical_clearance LIKE "%KET-872/UN2.F1/ETIK/PPM.00.02/2022%" THEN "KET-872/UN2.F1/ETIK/PPM.00.02/2022"
								WHEN nomor_ethical_clearance LIKE "%LB.02.01/KEP/042/2022%" THEN "LB.02.01/KEP/042/2022"
								WHEN nomor_ethical_clearance LIKE "%S-252/UN2.F1/ETIK/PPM.00.02/2024%" THEN "S-252/UN2.F1/ETIK/PPM.00.02/2024"
								WHEN nomor_ethical_clearance LIKE "%Studi Genomik pada Pasien Diabetes Melitus Awitan Usia Muda%" THEN "KET-872/UN2.F1/ETIK/PPM.00.02/2022"
								WHEN nama_peneliti_pi LIKE "%KET-872/UN2.F1/ETIK/PPM.00.02/2022%" THEN "KET-872/UN2.F1/ETIK/PPM.00.02/2022"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "%VII.14/LT/2022"
								)
								AND (judul_penelitian LIKE "ANALISIS GENOMIK PADA PSORIASIS%")
								AND YEAR(logged_at) < "2024" THEN "2774/UN14.2.2.VII.14/LT/2022"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "S-252/UN2.F1/ETIK/PPM.00.02/2024"
								)
								AND (judul_penelitian LIKE "%dan Mycobacterium%")
								AND YEAR(logged_at) >= "2024" THEN "S-252/UN2.F1/ETIK/PPM.00.02/2024"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "%VII.14/LT/2024"
								)
								AND (judul_penelitian LIKE "ANALISIS GENOMIK PADA PSORIASIS%")
								AND YEAR(logged_at) >= "2024" THEN "0512/UN14.2.2.VII.14/LT/2024"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "KET-42/UN2.F1/ETIK/PPM.00.02/2023"
								)
								AND (judul_penelitian LIKE "%dan Mycobacterium%")
								AND YEAR(logged_at) < "2024" THEN "KET-42/UN2.F1/ETIK/PPM.00.02/2023"
								WHEN (NOT nomor_ethical_clearance LIKE "%KET-1203/UN2.F1/ETIK/PPM.00.02/2023%")
								AND (judul_penelitian LIKE "%Studi Genomik pada Pasien Diabetes Melitus Dewasa%") THEN "KET-1203/UN2.F1/ETIK/PPM.00.02/2023"
								WHEN (NOT nomor_ethical_clearance LIKE "%286/KEPK/XII/2022%")
								AND (judul_penelitian LIKE "%GERCEP (Genomik Kanker untuk Pencegahan dan Terapi Presisi) BGSI Rumah Sakit Kanker Dharmais 2022 - 2023%") THEN "286/KEPK/XII/2022"
								WHEN (
									NOT nomor_ethical_clearance LIKE "%LB.02.01/KEP/042/2022%"
									OR nomor_ethical_clearance IS NULL
								)
								AND (judul_penelitian LIKE "%Clopidogrel Resistance Study in Ischemic Stroke of Indonesian Population%") THEN "LB.02.01/KEP/042/2022"
								ELSE nomor_ethical_clearance
							END AS nomor_ec,
							CASE
								WHEN nomor_ethical_clearance LIKE "%VII.14/LT/2024" THEN "ANALISIS GENETIK PADA PSORIASIS VULGARIS (Tahap 2)"
								WHEN nomor_ethical_clearance LIKE "%VII.14/LT/2022" THEN "ANALISIS GENETIK PADA PSORIASIS VULGARIS"
								WHEN nomor_ethical_clearance LIKE "%KEPK/XII/2022%" THEN "GERCEP (Genomik Kanker untuk Pencegahan dan Terapi Presisi) BGSI Rumah Sakit Kanker Dharmais 2022 - 2023"
								WHEN nomor_ethical_clearance LIKE "%KE/FK/0017/EC/2023" THEN "NEXT GENERATION SEQUENCING UNTUK PASIEN DUCHENNE MUSCULAR DYSTROPHY (DMD) di INDONESIA"
								WHEN nomor_ethical_clearance LIKE "KET-1203/UN2.F1/ETIK/PPM.00.02%" THEN "Studi Genomik pada Pasien Diabetes Melitus Dewasa"
								WHEN nomor_ethical_clearance LIKE "%KET-42/UN2.F1/ETIK/PPM.00.02/2023%" THEN "Analisis Genomik Manusia dan Mycobacterium Tuberculosis pada Kasus TBC Paru Sensitif dan Resisten Obat di Indonesia"
								WHEN nomor_ethical_clearance LIKE "%KET-872/UN2.F1/ETIK/PPM.00.02/2022%" THEN "Studi Genomik pada Pasien Diabetes Melitus Awitan Usia Muda"
								WHEN nomor_ethical_clearance LIKE "%LB.02.01/KEP/042/2022%" THEN "Clopidogrel Resistance Study in Ischemic Stroke of Indonesian Population"
								WHEN nomor_ethical_clearance LIKE "%S-252/UN2.F1/ETIK/PPM.00.02/2024%" THEN "Analisis Genomik Manusia dan Mycobacterium tuberculosis pada Kasus TBC Paru Sensitif dan Resisten Obat di Indonesia"
								WHEN nomor_ethical_clearance LIKE "%Studi Genomik pada Pasien Diabetes Melitus Awitan Usia Muda%" THEN "Studi Genomik pada Pasien Diabetes Melitus Awitan Usia Muda"
								WHEN nama_peneliti_pi LIKE "%KET-872/UN2.F1/ETIK/PPM.00.02/2022%" THEN "Studi Genomik pada Pasien Diabetes Melitus Awitan Usia Muda"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "%VII.14/LT/2022"
								)
								AND (judul_penelitian LIKE "ANALISIS GENOMIK PADA PSORIASIS%")
								AND YEAR(logged_at) < "2024" THEN "ANALISIS GENETIK PADA PSORIASIS VULGARIS"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "S-252/UN2.F1/ETIK/PPM.00.02/2024"
								)
								AND (judul_penelitian LIKE "%dan Mycobacterium%")
								AND YEAR(logged_at) >= "2024" THEN "Analisis Genomik Manusia dan Mycobacterium tuberculosis pada Kasus TBC Paru Sensitif dan Resisten Obat di Indonesia"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "%VII.14/LT/2024"
								)
								AND (judul_penelitian LIKE "ANALISIS GENOMIK PADA PSORIASIS%")
								AND YEAR(logged_at) >= "2024" THEN "ANALISIS GENETIK PADA PSORIASIS VULGARIS (Tahap 2)"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "KET-42/UN2.F1/ETIK/PPM.00.02/2023"
								)
								AND (judul_penelitian LIKE "%dan Mycobacterium%")
								AND YEAR(logged_at) < "2024" THEN "Analisis Genomik Manusia dan Mycobacterium tuberculosis pada Kasus TBC Paru Sensitif dan Resisten Obat di Indonesia"
								ELSE judul_penelitian
							END AS judul_penelitian,
							CASE
								WHEN nomor_ethical_clearance LIKE "%VII.14/LT/2024" THEN "Dr. dr. I G. N. Darmaputra, Sp.KK(K), FINSDV, FAADV"
								WHEN nomor_ethical_clearance LIKE "%VII.14/LT/2022" THEN "Dr. dr. I G. N. Darmaputra, Sp.KK(K), FINSDV, FAADV"
								WHEN nomor_ethical_clearance LIKE "%KEPK/XII/2022%" THEN "dr. Rizky Ifandriani Putri, Sp. PA"
								WHEN nomor_ethical_clearance LIKE "%KE/FK/0017/EC/2023" THEN "dr. Gunadi, Ph.D, Sp.BA"
								WHEN nomor_ethical_clearance LIKE "KET-1203/UN2.F1/ETIK/PPM.00.02%" THEN "dr. Dicky Levenus Tahapary, SpPD, K-EMD, Ph.D"
								WHEN nomor_ethical_clearance LIKE "%KET-42/UN2.F1/ETIK/PPM.00.02/2023%" THEN "dr. Pompini Agustina Sitompul, Sp.P (K)"
								WHEN nomor_ethical_clearance LIKE "%KET-872/UN2.F1/ETIK/PPM.00.02/2022%" THEN "dr. Dicky Levenus Tahapary, SpPD, K-EMD, Ph.D"
								WHEN nomor_ethical_clearance LIKE "%LB.02.01/KEP/042/2022%" THEN "dr. Mursyid Bustami, Sp.S(K), KIC, MARS"
								WHEN nomor_ethical_clearance LIKE "%S-252/UN2.F1/ETIK/PPM.00.02/2024%" THEN "dr. Pompini Agustina Sitompul, Sp.P (K)"
								WHEN nomor_ethical_clearance LIKE "%Studi Genomik pada Pasien Diabetes Melitus Awitan Usia Muda%" THEN "dr. Dicky Levenus Tahapary, SpPD, K-EMD, Ph.D"
								WHEN nama_peneliti_pi LIKE "%KET-872/UN2.F1/ETIK/PPM.00.02/2022%" THEN "dr. Dicky Levenus Tahapary, SpPD, K-EMD, Ph.D"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "%VII.14/LT/2022"
								)
								AND (judul_penelitian LIKE "ANALISIS GENOMIK PADA PSORIASIS%")
								AND YEAR(logged_at) < "2024" THEN "Dr. dr. I G. N. Darmaputra, Sp.KK(K), FINSDV, FAADV"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "S-252/UN2.F1/ETIK/PPM.00.02/2024"
								)
								AND (judul_penelitian LIKE "%dan Mycobacterium%")
								AND YEAR(logged_at) >= "2024" THEN "dr. Pompini Agustina Sitompul, Sp.P (K)"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "%VII.14/LT/2024"
								)
								AND (judul_penelitian LIKE "ANALISIS GENOMIK PADA PSORIASIS%")
								AND YEAR(logged_at) >= "2024" THEN "Dr. dr. I G. N. Darmaputra, Sp.KK(K), FINSDV, FAADV"
								WHEN (
									nomor_ethical_clearance IS NULL
									OR NOT nomor_ethical_clearance LIKE "KET-42/UN2.F1/ETIK/PPM.00.02/2023"
								)
								AND (judul_penelitian LIKE "%dan Mycobacterium%")
								AND YEAR(logged_at) < "2024" THEN "dr. Pompini Agustina Sitompul, Sp.P (K)"
								WHEN (NOT nomor_ethical_clearance LIKE "%286/KEPK/XII/2022%")
								AND (judul_penelitian LIKE "%GERCEP (Genomik Kanker untuk Pencegahan dan Terapi Presisi) BGSI Rumah Sakit Kanker Dharmais 2022 - 2023%") THEN "dr. Rizky Ifandriani Putri, Sp. PA"
								ELSE nama_peneliti_pi
							END AS nama_pi
						FROM
							(
								SELECT DISTINCT
									ehr_id id_subject,
									logged_at,
									REGEXP_REPLACE(
										TRIM(
											BOTH '"'
											FROM
												JSON_EXTRACT(composition, '$.composition."ethical-clearance-v1/episode_of_care_-_institution/nomor_ethical_clearance"')
										),
										'(^[ ]+)|[\\t\\n\\v\\f]+',
										''
									) nomor_ethical_clearance,
									REGEXP_REPLACE(
										TRIM(
											BOTH '"'
											FROM
												JSON_EXTRACT(composition, '$.composition."ethical-clearance-v1/episode_of_care_-_institution/judul_penelitian"')
										),
										'(^[ ]+)|[\\t\\n\\v\\f]+',
										''
									) judul_penelitian,
									REGEXP_REPLACE(
										TRIM(
											BOTH '"'
											FROM
												JSON_EXTRACT(composition, '$.composition."ethical-clearance-v1/episode_of_care_-_institution/nama_peneliti_pi"')
										),
										'(^[ ]+)|[\\t\\n\\v\\f]+',
										''
									) nama_peneliti_pi
								FROM
									regina_log
								WHERE
									template_id = 'ethical-clearance-v1'
							) t
						WHERE
							judul_penelitian IS NOT NULL
							AND nama_peneliti_pi IS NOT NULL
					) t
				GROUP BY
					1
			),
			phenovar_ec AS (
				SELECT DISTINCT
					id_subject,
					nomor_ec,
					judul_penelitian
				FROM
					(
						SELECT
							t1.id_subject,
							GROUP_CONCAT(DISTINCT t1.`value` SEPARATOR "; ") nomor_ec,
							GROUP_CONCAT(DISTINCT t2.research_title SEPARATOR "; ") judul_penelitian,
							COUNT(DISTINCT t1.`value`) ct_ec
						FROM
							phenovar_docs_rekrutmen_registrasi t1
							LEFT JOIN phenovar_ethical_clearance t2 ON t1.`value` = t2.ec_number
						WHERE
							t1.`path` = 'Nomor EC'
						GROUP BY
							1
						UNION ALL
						SELECT
							id_subject,
							GROUP_CONCAT(DISTINCT nomor_ec) nomor_ec,
							GROUP_CONCAT(DISTINCT judul_penelitian) judul_penelitian,
							COUNT(DISTINCT nomor_ec) ct_ec
						FROM
							(
								SELECT
									participant_id id_subject,
									REGEXP_REPLACE(REGEXP_SUBSTR(question_answer, "'Nomor EC': '[^'']+'"), "('Nomor EC': |')", "") nomor_ec,
									REGEXP_REPLACE(REGEXP_SUBSTR(question_answer, "'Judul Penelitian': '[^'']+'"), "('Judul Penelitian': |')", "") judul_penelitian
								FROM
									phenovar_documents
								WHERE
									institution_name = 'system'
							) t
						WHERE
							id_subject NOT IN("AAA00001", "AAA01410", "AAA01419", "AAA01420", "AAA01423", "AAA01424", "AAA01425", "AAA01427", "AAA01428", "AAA01429")
						GROUP BY
							1
					) t
			),
			-- Regina only has participant type for PAH
			regina_participant_type AS (
				SELECT
					id_subject,
					GROUP_CONCAT(DISTINCT participant_type) participant_type,
					COUNT(*) ct_type
				FROM
					(
						SELECT DISTINCT
							t1.ehr_id id_subject,
							CASE
								WHEN COALESCE(
									t2.corrected_participant_type,
									UPPER(
										TRIM(
											BOTH '"'
											FROM
												JSON_EXTRACT(t1.composition, '$.composition."genetic-disorders-pah-registrasi-v2/registrasi/identitas/tipe_pasien:0|value"')
										)
									)
								) = "PATIENT" THEN "PASIEN"
								ELSE NULL
							END participant_type
						FROM
							regina_log t1
							LEFT JOIN demography_corrected_participant_type t2 ON t1.ehr_id = t2.id_subject
						WHERE
							template_id = 'genetic-disorders-pah-registrasi-v2'
					) t
				GROUP BY
					1
			),
			phenovar_participant_type AS (
				SELECT
					id_subject,
					GROUP_CONCAT(DISTINCT participant_type) participant_type,
					COUNT(*) ct_type
				FROM
					(
						SELECT DISTINCT
							t1.id_subject,
							UPPER(COALESCE(t2.corrected_participant_type, t1.participant_type)) participant_type
						FROM
							(
								SELECT
									id_subject,
									`value` participant_type
								FROM
									phenovar_docs_rekrutmen_registrasi
								WHERE
									path = 'Koleksi Sampel|Tipe Partisipan'
								UNION ALL
								SELECT
									id_subject,
									`value` participant_type
								FROM
									phenovar_docs_rekrutmen_basic
								WHERE
									path = 'Admisi Partisipan|Tipe Partisipan'
								UNION ALL
								-- Also adding here for missing ones 
								SELECT DISTINCT
									id_subject,
									corrected_participant_type participant_type
								FROM
									demography_corrected_participant_type
							) t1
							LEFT JOIN demography_corrected_participant_type t2 ON t1.id_subject = t2.id_subject
					) t
				GROUP BY
					1
			),
			regina_demog AS (
				SELECT DISTINCT
					t1.nik,
					t1.id_subject regina_id_subject,
					t2.phenovar_id_subject,
					t1.birthdate birth_date,
					CASE
						WHEN t1.gender = 'Perempuan' THEN 'FEMALE'
						WHEN t1.gender = 'Laki-laki' THEN 'MALE'
					END sex,
					t4.provinsi,
					t1.creationDate creation_date,
					t3.nomor_ec nomor_ec,
					t3.judul_penelitian judul_penelitian
				FROM
					(
						SELECT
							-- Somehow same regina ID with different ehr_id. WERID! 
							*
						FROM
							(
								SELECT
									*,
									ROW_NUMBER() OVER (
										PARTITION BY
											id_subject,
											nik
										ORDER BY
											creationDate ASC
									) rn
								FROM
									dwh_restricted.decrypt_regina t1
							) t
						WHERE
							rn = 1
					) t1
					LEFT JOIN dwh_restricted.regina_migration t2 ON t1.id_subject = t2.id_subject_regina
					LEFT JOIN regina_ec t3 ON t1.id_subject = t3.id_subject
					LEFT JOIN dwh_restricted.regina_migration t4 ON t1.id_subject = t4.id_subject_regina
			),
			regina_demog2 AS (
				SELECT
					t1.nik,
					t1.regina_id_subject,
					t1.phenovar_id_subject,
					t1.birth_date,
					t1.sex,
					UPPER(t1.provinsi) province,
					t1.creation_date,
					COALESCE(t1.nomor_ec, t2.nomor_ec) nomor_ec,
					COALESCE(t1.judul_penelitian, t2.judul_penelitian) judul_penelitian,
					t3.participant_type
				FROM
					regina_demog t1
					LEFT JOIN (
						SELECT
							t1.nik,
							NULL AS regina_id_subject,
							t1.id_subject phenovar_id_subject,
							CASE
								WHEN t1.birth_date = "0976-05-19" THEN DATE("1976-05-19")
								ELSE t1.birth_date
							END birth_date,
							UPPER(t1.sex) sex,
							t1.created_at creation_date,
							t2.nomor_ec,
							t2.judul_penelitian
						FROM
							dwh_restricted.decrypted_phenovar_participants t1
							LEFT JOIN phenovar_ec t2 ON t1.id_subject = t2.id_subject
						WHERE
							t1.id_subject IN (
								SELECT DISTINCT
									phenovar_id_subject
								FROM
									regina_demog
								WHERE
									phenovar_id_subject IS NOT NULL
							)
					) t2 ON t1.phenovar_id_subject = t2.phenovar_id_subject
					LEFT JOIN regina_participant_type t3 ON t1.regina_id_subject = t3.id_subject
			),
			phenovar_demog1 AS (
				SELECT
					t1.nik,
					t1.full_name,
					NULL AS regina_id_subject,
					t1.id_subject phenovar_id_subject,
					CASE
						WHEN t1.birth_date = "0976-05-19" THEN DATE("1976-05-19")
						ELSE t1.birth_date
					END birth_date,
					UPPER(t1.sex) sex,
					UPPER(t1.province) province,
					t1.created_at creation_date,
					t2.nomor_ec nomor_ec,
					t2.judul_penelitian,
					t3.participant_type
				FROM
					dwh_restricted.decrypted_phenovar_participants t1
					LEFT JOIN phenovar_ec t2 ON t1.id_subject = t2.id_subject
					LEFT JOIN phenovar_participant_type t3 ON t1.id_subject = t3.id_subject
				WHERE
					NOT t1.id_subject IN (
						SELECT DISTINCT
							phenovar_id_subject
						FROM
							regina_demog
						WHERE
							phenovar_id_subject IS NOT NULL
					)
			),
			phenovar_duplicate_name_nik AS (
				SELECT
					full_name,
					nik,
					GROUP_CONCAT(phenovar_id_subject) duplicate_name_nik_id_subject,
					COUNT(*) ct
				FROM
					phenovar_demog1
				GROUP BY
					1,
					2
				HAVING
					ct > 1
			),
			phenovar_demog2 AS (
				SELECT
					t1.nik,
					t1.regina_id_subject,
					t1.phenovar_id_subject,
					t1.t1.birth_date,
					t1.sex,
					t1.province,
					t1.creation_date,
					t1.nomor_ec,
					t1.judul_penelitian,
					t1.participant_type,
					t2.duplicate_name_nik_id_subject
				FROM
					phenovar_demog1 t1
					LEFT JOIN phenovar_duplicate_name_nik t2 ON t1.nik = t2.nik
					AND t1.full_name = t2.full_name
			),
			all_demog AS (
				SELECT
					id_subject,
					regina_id_subject,
					phenovar_id_subject,
					sex,
					province,
					age_at_recruitment,
					creation_date,
					participant_type registry_participant_type,
					is_excluded,
					exclusion_reason,
					duplicate_name_nik_id_subject,
					GROUP_CONCAT(DISTINCT nomor_ec SEPARATOR "; ") nomor_ec,
					GROUP_CONCAT(DISTINCT judul_penelitian SEPARATOR "; ") judul_penelitian
				FROM
					(
						SELECT DISTINCT
							t1.*,
							CASE
								WHEN FLOOR(DATEDIFF(creation_date, birth_date) / 365.25) < 0 THEN 0
								ELSE FLOOR(DATEDIFF(creation_date, birth_date) / 365.25)
							END age_at_recruitment,
							CASE
								WHEN t2.id_subject IS NOT NULL THEN TRUE
								ELSE FALSE
							END is_excluded,
							t2.reason exclusion_reason,
							COALESCE(t1.regina_id_subject, t1.phenovar_id_subject) id_subject
						FROM
							(
								SELECT DISTINCT
									*,
									NULL duplicate_name_nik_id_subject
								FROM
									regina_demog2
								UNION ALL
								SELECT DISTINCT
									*
								FROM
									phenovar_demog2
							) t1
							LEFT JOIN demography_exclusion t2 ON (
								t1.regina_id_subject = t2.id_subject
								OR t1.phenovar_id_subject = t2.id_subject
							)
					) t
				GROUP BY
					1,
					2,
					3,
					4,
					5,
					6,
					7,
					8,
					9
			),
			# For RSPI and RSUP we get only from the simbiox since registry doesn't contain information whether a patient blood will be collected esp. in RegINA. We do have it in phenovar, though (inside IC).
			# As discussed with Fitria from biobank team, we can list the participant who have either buffycoat, plasma or whoole blood
			sb_bio AS (
				SELECT
					t2.id_subject,
					t7.patient_categ,
					t3.biobank_nama,
					t1.origin_code_repository,
					t1.code_repository,
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
						SELECT
							px_id,
							GROUP_CONCAT(DISTINCT patient_categ SEPARATOR "; ") patient_categ,
							COUNT(DISTINCT patient_categ) ct_patient_type
						FROM
							(
								SELECT
									px_id,
									CASE
										WHEN kode_icd LIKE "z00%" THEN "KONTROL"
										ELSE "PASIEN"
									END patient_categ
								FROM
									simbiox_log_visit_biospc
							) t
						GROUP BY
							1
					) t7 ON t7.px_id = t2.id_patient
					LEFT JOIN master_biobank t3 ON t1.id_biobank = t3.id_biobank
					LEFT JOIN master_status t4 ON t1.biosample_status = t4.id
					LEFT JOIN master_specimen t5 ON t1.biosample_specimen = t5.id
					LEFT JOIN master_sample_type t6 ON t1.biosample_type = t6.id
			),
			agg_biosample AS (
				SELECT
					id_subject,
					GROUP_CONCAT(DISTINCT biobank_nama SEPARATOR "; ") biobank_nama,
					GROUP_CONCAT(DISTINCT origin_code_repository SEPARATOR "; ") origin_code_repository,
					SUM(is_buffycoat) count_buffycoat,
					SUM(is_plasma) count_plasma,
					SUM(is_urin) count_urin,
					SUM(is_dna) count_dna,
					SUM(is_whole_blood) count_whole_blood,
					SUM(is_serum) count_serum,
					SUM(is_sputum) count_sputum,
					SUM(is_isolat) count_isolat,
					SUM(is_airmata) count_airmata,
					SUM(is_jaringan) count_jaringan
				FROM
					sb_bio
				GROUP BY
					1
			),
			agg_biosample_tb AS (
				SELECT
					id_subject,
					GROUP_CONCAT(DISTINCT biobank_nama SEPARATOR "; ") biobank_nama,
					SUM(is_buffycoat) count_buffycoat,
					SUM(is_plasma) count_plasma,
					SUM(is_urin) count_urin,
					SUM(is_dna) count_dna,
					SUM(is_whole_blood) count_whoole_blood,
					SUM(is_serum) count_serum,
					SUM(is_sputum) count_sputum,
					SUM(is_isolat) count_isolat,
					SUM(is_airmata) count_airmata,
					SUM(is_jaringan) count_jaringan
				FROM
					sb_bio
				WHERE
					biobank_nama IN ('Biobank RSPI Prof. Dr. Sulianti Saroso', 'Biobank RSUP Persahabatan')
				GROUP BY
					1
			),
			tb_particip AS (
				SELECT
					id_subject,
					CASE
						WHEN biobank_nama = 'Biobank RSPI Prof. Dr. Sulianti Saroso' THEN 'RSPI Prof. Dr. Sulianti Saroso'
						WHEN biobank_nama = 'Biobank RSUP Persahabatan' THEN 'RSUP Persahabatan'
						-- choose only one of the biobank
						ELSE REGEXP_REPLACE(biobank_nama, '(?i)(Biobank |; .+)', '')
					END hospital_name
				FROM
					agg_biosample_tb
				WHERE
					(
						count_plasma != 0
						OR count_whoole_blood != 0
						OR count_buffycoat != 0
					)
					-- This participant should be belong to RSCM
					AND NOT id_subject IN ('AAA01156')
			),
			all_demog2 AS (
				SELECT
					-- 			"Regina" categ,
					id_subject,
					hospital_name,
					has_tiered_digital_consent,
					is_excluded
				FROM
					(
						SELECT
							ROW_NUMBER() OVER (
								PARTITION BY
									ehr_id
								ORDER BY
									logged_at ASC
							) rn,
							ehr_id id_subject,
							CASE
								WHEN hospital = 'RS_KANKER_DHARMAIS' THEN 'RS Kanker Dharmais'
								WHEN hospital = 'RS_PERSAHABATAN' THEN 'RSUP Persahabatan'
								WHEN hospital = 'RS_PON' THEN 'RS PON Prof. Dr. dr. Mahar Mardjono'
								WHEN hospital = 'RS_SANGLAH' THEN 'RSUP Prof. Dr. IGNG Ngoerah Denpasar'
								WHEN hospital = 'RS_SARDJITO' THEN 'RSUP Dr. Sardjito'
								WHEN hospital = 'RS_SULIANTI_SAROSO' THEN 'RSPI Prof. Dr. Sulianti Saroso'
								WHEN hospital = 'RSCM' THEN 'RS Dr. Cipto Mangunkusumo'
								ELSE NULL
							END hospital_name,
							-- RegINA does not have tiered digital consent
							FALSE has_tiered_digital_consent,
							CASE
								WHEN hospital = 'RS_PERSAHABATAN'
								OR hospital = 'RS_SULIANTI_SAROSO' THEN TRUE
								ELSE FALSE
							END is_excluded
						FROM
							regina_log
						WHERE
							template_id IN ('demography-v2')
							AND NOT ehr_id IN (
								SELECT
									id_subject
								FROM
									tb_particip
							)
					) t
				WHERE
					t.rn = 1
				UNION ALL
				SELECT
					-- 			"TB" categ,
					tb_particip.id_subject,
					tb_particip.hospital_name,
					-- Regina does not have tiered. TB included.
					FALSE has_tiered_digital_consent,
					FALSE is_excluded
				FROM
					tb_particip
					LEFT JOIN phenovar_digital_consent ON tb_particip.id_subject = phenovar_digital_consent.id_subject
				UNION ALL
				SELECT
					-- phenovar
					phenovar_participants.id_subject,
					phenovar_participants.hospital_name,
					CASE
						WHEN phenovar_digital_consent.id_subject IS NULL THEN FALSE
						ELSE TRUE
					END has_tiered_digital_consent,
					CASE
						WHEN hospital_name = 'RSUP Persahabatan'
						OR hospital_name = 'RSPI Prof. Dr. Sulianti Saroso' THEN TRUE
						ELSE FALSE
					END is_excluded
				FROM
					phenovar_participants
					LEFT JOIN phenovar_digital_consent ON phenovar_participants.id_subject = phenovar_digital_consent.id_subject
				WHERE
					NOT phenovar_participants.id_subject IN (
						SELECT
							id_subject
						FROM
							tb_particip
					)
			)
			-- SELECT * FROM tb_particip	
,
			biobank_participant_type AS (
				SELECT
					t2.id_subject,
					t1.patient_categ participant_type
				FROM
					(
						SELECT
							px_id,
							CASE
								WHEN kode_icd LIKE "z00%" THEN "KONTROL"
								ELSE "PASIEN"
							END patient_categ,
							ROW_NUMBER() OVER (
								PARTITION BY
									px_id
								ORDER BY
									datevisit DESC
							) rn
						FROM
							simbiox_log_visit_biospc
						ORDER BY
							rn DESC
					) t1
					LEFT JOIN simbiox_patients t2 ON t1.px_id = t2.id_patient
				WHERE
					rn = 1
			),
			sequncing_and_binf_status AS (
				SELECT
					id_subject,
					sequencer,
					CASE
						WHEN is_sequenced IS NULL THEN FALSE
						ELSE TRUE
					END is_wgs_sequenced,
					CASE
						WHEN is_secondary_analysed IS NULL THEN FALSE
						ELSE TRUE
					END is_wgs_secondary_analysed
				FROM
					(
						SELECT
							id_subject,
							GROUP_CONCAT(DISTINCT sequencer SEPARATOR "; ") sequencer,
							GROUP_CONCAT(DISTINCT date_primary SEPARATOR "; ") is_sequenced,
							GROUP_CONCAT(DISTINCT date_secondary SEPARATOR "; ") is_secondary_analysed
						FROM
							gold_qc
						WHERE
							id_subject IS NOT NULL
						GROUP BY
							1
					) t
			),
			# 2025 target
			target AS (
				SELECT
					"BGSi Central" hospital_name,
					2468 target
				UNION ALL
				SELECT
					"PKIAN RSAB Harapan Kita" hospital_name,
					1500 target
				UNION ALL
				SELECT
					"RS Dr. Cipto Mangunkusumo" hospital_name,
					3800 target
				UNION ALL
				SELECT
					"RS Kanker Dharmais" hospital_name,
					4800 target
				UNION ALL
				SELECT
					"RS PON Prof. Dr. dr. Mahar Mardjono" hospital_name,
					1950 target
				UNION ALL
				SELECT
					"RSPI Prof. Dr. Sulianti Saroso" hospital_name,
					1250 target
				UNION ALL
				SELECT
					"RSPJD Harapan Kita" hospital_name,
					1000 target
				UNION ALL
				SELECT
					"RSUP Dr. Sardjito" hospital_name,
					900 target
				UNION ALL
				SELECT
					"RSUP Persahabatan" hospital_name,
					1250 target
				UNION ALL
				SELECT
					"RSUP Prof. Dr. IGNG Ngoerah Denpasar" hospital_name,
					1400 target
			)
		SELECT
			*
		FROM
			(
				SELECT DISTINCT
					ROW_NUMBER() OVER (
						PARTITION BY
							t1.id_subject,
							t2.hospital_name,
							t1.nomor_ec
						ORDER BY
							t1.creation_date
					) rn,
					t1.id_subject,
					t1.regina_id_subject,
					t1.phenovar_id_subject,
					t1.duplicate_name_nik_id_subject,
					t2.hospital_name,
					t1.sex,
					CASE
						WHEN t1.province = "YOGYAKARTA" THEN "DAERAH ISTIMEWA YOGYAKARTA"
						WHEN t1.province = "RIAU" THEN "KEPULAUAN RIAU"
						WHEN t1.province = "KEP. BANGKA BELITUNG" THEN "KEPULAUAN BANGKA BELITUNG"
						WHEN t1.province = "-"
						OR t1.province = "" THEN NULL
						ELSE t1.province
					END province,
					t1.age_at_recruitment,
					t1.creation_date,
					t1.registry_participant_type,
					t3.participant_type biobank_participant_type,
					CASE
						WHEN (
							(
								t1.judul_penelitian = "Survei Kesehatan Indonesia (SKI) 2023"
								AND TRIM(COALESCE(t1.registry_participant_type, t3.participant_type)) != "POPULASI UMUM"
							)
							OR (
								(
									t1.judul_penelitian != "Survei Kesehatan Indonesia (SKI) 2023"
									OR t1.judul_penelitian != "Survei Kesehatan Indonesia (SKI) 2023" IS NULL
								)
								AND t2.hospital_name = "BGSi Central"
								AND t3.participant_type = "KONTROL"
							)
						) THEN "POPULASI UMUM"
						WHEN (
							t1.judul_penelitian != "Survei Kesehatan Indonesia (SKI) 2023"
							OR t1.judul_penelitian IS NULL
						)
						AND TRIM(COALESCE(t1.registry_participant_type, t3.participant_type)) = "POPULASI UMUM" THEN "KONTROL"
						ELSE TRIM(COALESCE(t1.registry_participant_type, t3.participant_type))
					END participant_type,
					t1.nomor_ec,
					t1.judul_penelitian,
					t2.has_tiered_digital_consent,
					-- This only for dashboard purpose (Exlcluding TB participant)
					CASE
						WHEN t2.is_excluded IS TRUE THEN TRUE
						WHEN t1.is_excluded IS NOT NULL THEN t1.is_excluded
						ELSE FALSE
					END is_excluded,
					CASE
						WHEN t2.is_excluded IS TRUE THEN "TB Exclusion due to not having BC/Plasma"
						ELSE t1.exclusion_reason
					END exclusion_reason,
					-- For dashboard purposes
					CASE
						WHEN t1.judul_penelitian LIKE "%(AMI) Polygenic Risk Score (PRS)%"
						AND NOT t1.judul_penelitian LIKE "%;%" THEN "Validation of Acute Myocardial Infarction (AMI) Polygenic Risk Score (PRS) Using Whole Genome Sequencing (WGS) for Indonesian Population"
						WHEN t1.judul_penelitian LIKE "%ANALISIS GENETIK PADA PSORIASIS VULGARIS%"
						AND NOT t1.judul_penelitian LIKE "%;%" THEN "ANALISIS GENETIK PADA PSORIASIS VULGARIS"
						WHEN t1.judul_penelitian LIKE "%bacterium%"
						AND NOT t1.judul_penelitian LIKE "%;%" THEN "Analisis Genomik Manusia & Mycobacterium Tuberculosis pada Kasus TBC Paru Sensitif dan Resistensi Obat di Indonesia"
						WHEN t1.judul_penelitian LIKE "%Pulmonary Hypertension and Congenital Heart Disease in Adult Patients%"
						AND NOT t1.judul_penelitian LIKE "%;%" THEN "Determinant and Prognostic Factors (Clinical, Mollecular, and Genetics) of Pulmonary Hypertension and Congenital Heart Disease in Adult Patients"
						WHEN t1.judul_penelitian LIKE "%GERCEP (Genomik Kanker untuk Pencegahan dan Terapi Presisi)%"
						AND NOT t1.judul_penelitian LIKE "%;%" THEN "GERCEP (Genomik Kanker untuk Pencegahan dan Terapi Presisi) BGSI Rumah Sakit Kanker Dharmais 2022 - 2023"
						WHEN t1.judul_penelitian LIKE "%Studi Genomik pada Pasien Diabetes Melitus Dewasa%"
						AND NOT t1.judul_penelitian LIKE "%;%" THEN "Studi Genomik Multisenter pada Pasien Diabetes Mellitus (Dewasa)"
						WHEN t1.judul_penelitian LIKE "%Studi Genomik pada Pasien Diabetes Melitus Awitan Usia Muda%"
						AND NOT t1.judul_penelitian LIKE "%;%" THEN "Studi Genomik Multisenter pada Pasien Diabetes Mellitus (Awitan Usia Muda)"
						ELSE t1.judul_penelitian
					END corrected_judul_penelitian,
					CASE
						WHEN t4.count_buffycoat IS NULL THEN 0
						ELSE t4.count_buffycoat
					END count_buffycoat,
					CASE
						WHEN t4.count_plasma IS NULL THEN 0
						ELSE t4.count_plasma
					END count_plasma,
					CASE
						WHEN t4.count_whole_blood IS NULL THEN 0
						ELSE t4.count_whole_blood
					END count_whole_blood,
					CASE
						WHEN t4.count_serum IS NULL THEN 0
						ELSE t4.count_serum
					END count_serum,
					CASE
						WHEN t4.count_sputum IS NULL THEN 0
						ELSE t4.count_sputum
					END count_sputum,
					CASE
						WHEN t5.is_wgs_sequenced IS NULL THEN FALSE
						ELSE t5.is_wgs_sequenced
					END is_wgs_sequenced,
					CASE
						WHEN t5.is_wgs_secondary_analysed IS NULL THEN FALSE
						ELSE t5.is_wgs_secondary_analysed
					END is_wgs_secondary_analysed,
					t4.origin_code_repository,
					t6.target hub_target
				FROM
					all_demog t1
					LEFT JOIN all_demog2 t2 ON t1.id_subject = t2.id_subject
					LEFT JOIN biobank_participant_type t3 ON t1.id_subject = t3.id_subject
					LEFT JOIN agg_biosample t4 ON t1.id_subject = t4.id_subject
					LEFT JOIN sequncing_and_binf_status t5 ON t1.id_subject = t5.id_subject
					LEFT JOIN target t6 ON t2.hospital_name = t6.hospital_name
			) t
		WHERE
			t.rn = 1
	);
COMMIT;