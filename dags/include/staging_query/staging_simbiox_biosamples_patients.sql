/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is created to provide a materialised table on simbiox data stitched with the registry data for staging purpose.
-- Author   :   Abdullah Faqih
-- Created  :   16-02-2025
-- Changes:
---------------------------------------------------------------------------------------------------------------------------------
*/

DELETE FROM staging_simbiox_biosamples_patients;
INSERT INTO staging_simbiox_biosamples_patients (
		WITH sbp AS (
			SELECT sb.id_patient,
				sb.code_repository,
				id_mpi,
				id_subject,
				# Taking from patient for valid biobank origin as a first option, then from biosample.
				COALESCE(sp2.biobank_nama, sb.biobank_nama) biobank_nama
			FROM (
					SELECT # Several code_repository can have several biobank ids. Examples: 
						#	05000010401 -> Biobank RSPI Prof. Dr. Sulianti Saroso Biobank RSUP Persahabatan Biobank RSPI Prof. Dr. Sulianti Saroso
						# In addition several code_repo have different biosample status (2 and 3: active and transferred) causing duplicaiton rows. Examples:  ("05002210401", "05002210401", "0I0026001C01") #status 2 and status 3 causing
						# Biobank PKIAN RSAB Harapan Kita Biobank PKIAN RSAB Harapan Kita
						# Biobank RSPI Prof. Dr. Sulianti Saroso Biobank RSPI Prof. Dr. Sulianti Saroso
						# Here, we do distinct for the GROUP_CONCAT to get rid the same record (with different biosmaple status coming from the same biobank id. 
						# Our goal to have the same number of rows as the 'seq' above. - OK
						id_patient,
						code_repository,
						GROUP_CONCAT(DISTINCT mb.biobank_nama SEPARATOR ' ') biobank_nama
					FROM simbiox_biosamples sb
						LEFT JOIN master_biobank mb ON sb.id_biobank = mb.id_biobank
					WHERE sb.id_biobank NOT IN('1', '80dc7749-63f2-4a3a-9486-562f8a677333') #  exclude ('Biobank Sentral (BB Binomika)', 'Biobank Pusat')
					GROUP BY id_patient,
						code_repository
				) sb
				LEFT JOIN (
					SELECT # simbiox patients also have biobank nama. 
						# We assume that biobank nama inside the patient subject is where the biosample is originate. 
						# Therefore, for the downstream analysis, the simbiox patient biobank_nama will be used as the origin of biobank. 
						id_patient,
						# Here we do cleansing to (hopefully) remove any typos 
						# example that you can run to remove " ' and any spaces.
						# 		SET @var = " 	\"coba gan '\" ";
						# 		SELECT @var,  TRIM(REGEXP_REPLACE(@var,  "[\'\"]", ""))
						TRIM(REGEXP_REPLACE(id_mpi, "[\'\"]", "")) id_mpi,
						TRIM(REGEXP_REPLACE(id_subject, "[\'\"]", "")) id_subject,
						sex,
						mb.biobank_nama
					FROM simbiox_patients sp
						LEFT JOIN master_biobank mb ON sp.id_biobank = mb.id_biobank
				) sp2 ON sb.id_patient = sp2.id_patient
		)
		SELECT sbp.*,
			COALESCE(rd.id_subject, pp.id_subject) registry_id_subject,
			COALESCE(rd.sex, pp.sex) registry_sex
		FROM sbp
			LEFT JOIN regina_demography rd ON sbp.id_subject = rd.id_subject
			LEFT JOIN phenovar_participants pp ON sbp.id_subject = pp.id_subject
	)