/*
 ---------------------------------------------------------------------------------------------------------------------------------
 -- Purpose  :   This query is intended to be used as the staging results of all sequencer platform primary analysis data.
 -- Author   :   Abdullah Faqih
 -- Created  :   14-02-2025
 -- Changes	 :	 01-03-2025 Enforce the SKI code repo into the new id repo. SKI should be the origin_code_repo not the id_repo itself. 
						    Fix the windowing of Illumina data based on id_lib into only based on id_repository (just quick fix since prev the id_lib nulls) 
				 01-03-2025 Add new criteria for WFHV samples to at least have 9GB of total passed bases.
				 02-03-2025 Adding filter for testing id repositories
 ---------------------------------------------------------------------------------------------------------------------------------
 */
-- Your SQL code goes here
DELETE FROM staging_seq;
INSERT INTO staging_seq
(
	SELECT
		COALESCE(db_mgi.new_repository, seq_zlims.id_repository) id_repository,
		id_flowcell id_library,
		'MGI' sequencer,
		date_create date_primary,
		NULL sum_of_total_passed_bases,
		NULL sum_of_bam_size,
		COALESCE(db_mgi.new_index, seq_zlims.id_index) id_index
	FROM
		zlims_samples seq_zlims
		LEFT JOIN (
			# This zlims is separated since the dynamodb contains redundant rows for MGI data where a code repo could have two rows where one contain index and one not.
			SELECT
				dbt1.id_repository,
				dbt1.new_repository,
				dbt2.id_zlims_index,
				dbt2.new_index
			FROM
				(
					SELECT DISTINCT
						id_repository,
						new_repository
					FROM
						dynamodb_fix_id_repository_latest dbt1
					WHERE
						sequencer = "MGI"
				) dbt1
				LEFT JOIN (
					SELECT DISTINCT
						id_repository,
						id_zlims_index,
						new_index
					FROM
						dynamodb_fix_id_repository_latest
					WHERE
						sequencer = "MGI"
						AND (
							id_zlims_index IS NOT NULL
							AND new_index IS NOT NULL
						)
				) dbt2 ON dbt1.id_repository = dbt2.id_repository
		) db_mgi ON seq_zlims.id_repository = db_mgi.id_repository
	UNION ALL
	SELECT
		COALESCE(sfki.code_repository, t.id_repository) id_repository,
		id_library,
		sequencer,
		date_primary,
		NULL sum_of_total_passed_bases,
		NULL sum_of_bam_size,
		NULL id_index
	FROM
		(
			SELECT
				-- This windowing is needed due to reupload the same file within BSSH.
				ROW_NUMBER() OVER (
					PARTITION BY
						seq_ica.clean_id_repository
					ORDER BY
						seq_ica.time_modified DESC
				) rn,
				COALESCE(db_ica.new_repository, seq_ica.clean_id_repository) id_repository,
				id_library,
				'Illumina' sequencer,
				time_modified date_primary
			FROM
				(
				SELECT
					CASE
					-- DRAGEN
						WHEN id_repository LIKE "%DRAGEN%" THEN REGEXP_SUBSTR(id_repository, "[\\w\\d]+")
						-- TOP UP 
						WHEN id_repository LIKE "%_M" THEN REGEXP_SUBSTR(id_repository, "[A-Za-z0-9]+")
						ELSE id_repository
					END clean_id_repository,
					COALESCE(REGEXP_SUBSTR(TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(sample_list_technical_tags, '''bssh.run.name:LP.* '''), "[\'\",]", "")), "LP.+"), TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(tag_user_tags, '''LP.+?'''), "[\'\"]", ""))) id_library,
					time_modified
				FROM
					ica_samples				
				) seq_ica
				LEFT JOIN (
					SELECT DISTINCT
						id_repository,
						new_repository
					FROM
						dynamodb_fix_id_repository_latest
					WHERE
						sequencer = "Illumina"
				) db_ica ON seq_ica.clean_id_repository = db_ica.id_repository
		) t
	LEFT JOIN staging_fix_ski_id_repo sfki ON t.id_repository = sfki.new_origin_code_repository
	WHERE
		rn = 1
	UNION ALL
	SELECT
		COALESCE(db_wfhv.new_repository, seq_wfhv.id_repository) id_repository,
		id_batch id_library,
		"ONT" sequencer,
		date_upload date_primary,
		sum_of_total_passed_bases,
		sum_of_bam_size,
		NULL id_index
	FROM
		wfhv_samples seq_wfhv
		LEFT JOIN (
			SELECT DISTINCT
				id_repository,
				new_repository
			FROM
				dynamodb_fix_id_repository_latest
			WHERE
				sequencer = "ONT"
		) db_wfhv ON seq_wfhv.id_repository = db_wfhv.id_repository	
	WHERE
		CAST(seq_wfhv.sum_of_total_passed_bases AS DOUBLE ) >= 9*10e9 AND (NOT REGEXP_LIKE(seq_wfhv.id_repository, "(?i)(demo|test|benchmark|dev)"))
)