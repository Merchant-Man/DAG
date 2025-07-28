START TRANSACTION;

DELETE FROM staging_demography;

INSERT INTO
	staging_demography (
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
			# For RSPI and RSUP we get only from the simbiox since registry doesn't contain information whether a patient blood will be collected esp. in RegINA. We do have it in phenovar, though (inside IC).
			# As discussed with Fitria from biobank team, we can list the participant who have either buffycoat, plasma or whoole blood
			sb_bio AS (
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
			agg_biosample_tb AS (
				SELECT
					id_subject,
					biobank_nama,
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
					1,
					2
			),
			tb_particip AS (
				SELECT
					id_subject,
					CASE
						WHEN biobank_nama = 'Biobank RSPI Prof. Dr. Sulianti Saroso' THEN 'RSPI Prof. Dr. Sulianti Saroso'
						WHEN biobank_nama = 'Biobank RSUP Persahabatan' THEN 'RSUP Persahabatan'
					END hospital_name
				FROM
					agg_biosample_tb
				WHERE
					count_plasma != 0
					OR count_whoole_blood != 0
					OR count_buffycoat != 0
			),
			all_demog2 AS (
				SELECT DISTINCT
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
					END hospital_name
				FROM
					regina_log
				WHERE
					hospital != 'RS_PERSAHABATAN'
					AND hospital != 'RS_SULIANTI_SAROSO'
				UNION ALL
				SELECT
					phenovar_participants.id_subject,
					phenovar_participants.hospital_name
				FROM
					phenovar_participants
					LEFT JOIN phenovar_digital_consent ON phenovar_participants.id_subject = phenovar_digital_consent.id_subject
				WHERE
					hospital_name != 'RSUP Persahabatan'
					AND hospital_name != 'RSPI Prof. Dr. Sulianti Saroso'
					AND phenovar_digital_consent.id_subject IS NOT NULL
					# Excluding central participant SKI that don't have samples
					AND NOT phenovar_participants.id_subject IN (
						'AAA01326',
						'AAA01533',
						'AAA01543',
						'AAA01545',
						'AAA01548',
						'AAA01551',
						'AAA01558',
						'AAA01560',
						'AAA01561',
						'AAA01562',
						'AAA01565',
						'AAA01566',
						'AAA01626',
						'AAA01628',
						'AAA01630',
						'AAA01631',
						'AAA01632',
						'AAA01643',
						'AAA01644',
						'AAA01650',
						'AAA01681',
						'AAA02125',
						'AAA02129',
						'AAA02280',
						'AAA02422',
						'AAA02752',
						'AAA02756',
						'AAA03016',
						'AAA03017',
						'AAA03018',
						'AAA04675',
						'AAA05023',
						'AAA07162',
						'AAA07177',
						'AAA07491',
						'AAA08419',
						'AAA08430',
						'AAA08434',
						'AAA08450'
					)
				UNION ALL
				SELECT
					id_subject,
					hospital_name
				FROM
					tb_particip
			)
		SELECT
			all_demog2.*,
			all_demog.creation_date,
			all_demog.sex,
			all_demog.age_at_recruitment
		FROM
			all_demog2
			LEFT JOIN all_demog ON all_demog2.id_subject = all_demog.id_subject
	);
COMMIT;