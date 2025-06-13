/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended as dashboard for QC of sequencing data.
-- Author   :   Abdullah Faqih
-- Created  :   13-06-2025
-- Changes	: 
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here 
DROP TABLE IF EXISTS gold_sheets_sequencing;

CREATE TABLE gold_sheets_sequencing AS (
	WITH
		temp_data AS (
			SELECT
				t.*,
				CASE
					WHEN COALESCE(t3.id_subject, t4.id_subject) IS NULL THEN "Not In Registry"
					ELSE "In Registry"
				END registry_status,
				t2.id_subject,
				t2.origin_code_repository,
				ROW_NUMBER() OVER (
					PARTITION BY
						platform,
						t2.origin_code_repository
					ORDER BY
						extraction_status,
						extraction_date
				) rn
			FROM
				(
					SELECT
						COALESCE(sfki.code_repository, t1.code_repository) code_repository,
						t1.platform,
						t1.extraction_status,
						t1.libprep_status,
						t1.sequencing_finish_date,
						t1.sequencing_status,
						t1.volume_remain,
						t1.delivery_date,
						t1.extraction_date
					FROM
						sheets_sequencing t1
						LEFT JOIN staging_fix_ski_id_repo sfki ON t1.code_repository = sfki.new_origin_code_repository
				) t
				LEFT JOIN staging_simbiox_biosamples_patients t2 ON t.code_repository = t2.code_repository
				LEFT JOIN regina_demography t3 ON t2.id_subject = t3.id_subject
				LEFT JOIN phenovar_participants t4 ON t2.id_subject = t4.id_subject
		)
	SELECT
		platform,
		code_repository,
		extraction_status,
		libprep_status,
		sequencing_finish_date,
		sequencing_status,
		volume_remain,
		delivery_date,
		extraction_date,
		registry_status,
		id_subject,
		origin_code_repository,
		CASE
			WHEN rn > 1
			AND origin_code_repository IS NOT NULL THEN "Duplicate"
			ELSE NULL
		END duplicate_origin_code_repo_status
	FROM
		temp_data
)