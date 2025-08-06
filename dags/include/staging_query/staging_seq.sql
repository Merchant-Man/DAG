/*
 ---------------------------------------------------------------------------------------------------------------------------------
 -- Purpose  :   This query is intended to be used as the staging results of all sequencer platform primary analysis data.
 -- Author   :   Abdullah Faqih
 -- Created  :   14-02-2025
 -- Changes	 :	 01-03-2025 Abdullah Faqih - Enforce the SKI code repo into the new id repo. SKI should be the origin_code_repo not the id_repo itself. 
						    					Fix the windowing of Illumina data based on id_lib into only based on id_repository (just quick fix since prev the id_lib nulls) 
				 01-03-2025 Abdullah Faqih - Add new criteria for WFHV samples to at least have 9GB of total passed bases.
				 02-03-2025 Abdullah Faqih - Adding filter for testing id repositories.
				 13-03-2025 Abdullah Faqih - Adding window function for filtering multiple entries of samples due to top up.
				 15-03-2025 Abdullah Faqih - Adding filter for testing id repositories for illumina.
				 29-04-2025 Abdullah Faqih - Adding ztron pro samples
				 07-06-2025 Abdullah Faqih - Adding filter for ICA Sample by filtering out non "AVAILABLE" Samples s.t. any arbitary samples written by the users will not be included.
				 26-06-2025 Abdullah Faqih - Excluding the SKI samples that do not have informed consent. Currently only apply to the Illumina data. Should reduce 102 SKI samples.
				 30-06-2025 Abdullah Faqih - Adding transcation lock to the query
				 04-08-2025 Renata Triwijaya - Separation of dynamodb-fix based on fix_type
												Adding fix feature for ztronpro samples
 ---------------------------------------------------------------------------------------------------------------------------------
 */
-- Your SQL code goes here
START TRANSACTION;
DELETE FROM staging_seq;
INSERT INTO staging_seq 
(
	WITH
	-- Consolidated DynamoDB fixes for MGI
		db_mgi_fixes AS (
			SELECT
				id_repository
				, NULLIF(id_library, 'EMPTY') AS id_library
				, MAX(new_repository) AS new_repository
				, MAX(id_zlims_index) AS id_zlims_index
				, MAX(new_index) AS new_index
				, MAX(new_library) AS new_library
			FROM (
				SELECT
					id_repository
					, id_library
					, CASE WHEN fix_type = 'id_repository' THEN new_repository END AS new_repository
					, CASE WHEN fix_type = 'id_zlims_index' THEN id_zlims_index END AS id_zlims_index
					, CASE WHEN fix_type = 'id_zlims_index' THEN new_index END AS new_index
					, CASE WHEN fix_type = 'id_library' THEN new_library END AS new_library
					, ROW_NUMBER() OVER (
						PARTITION BY id_repository, id_library, fix_type
						ORDER BY time_requested DESC
					) AS rn
				FROM dynamodb_fix_samples
				WHERE sequencer = 'MGI'
			) ranked
			WHERE rn = 1
			GROUP BY id_repository, NULLIF(id_library, 'EMPTY')
		),
		
		-- List of samples to exclude in ztronpro (e.g., old hg37)
		deleted_hg37 AS (
			SELECT REGEXP_SUBSTR(original_sample_id, "[^_]+") code_repository
			FROM ztronpro_samples
			WHERE REGEXP_LIKE(original_sample_id, "(?i)_hg38")
				AND NOT original_sample_id LIKE "Ashkenazim%"
		),

		-- Unified MGI metadata from zlims and ztronpro
		mgi AS (
			SELECT
				id_repository
				, id_library
				, sequencer
				, date_primary
				, sum_of_total_passed_bases
				, sum_of_bam_size
				, id_index
			FROM (
				-- zlims_samples
				SELECT
					ROW_NUMBER() OVER (
						PARTITION BY COALESCE(db_mgi.new_repository, seq_zlims.id_repository)
						ORDER BY seq_zlims.date_create DESC
					) rn
					, COALESCE(db_mgi.new_repository, seq_zlims.id_repository) AS id_repository
					, COALESCE(db_mgi.new_library, seq_zlims.id_flowcell) AS id_library
					, 'MGI' AS sequencer
					, seq_zlims.date_create AS date_primary
					, NULL AS sum_of_total_passed_bases
					, NULL AS sum_of_bam_size
					, COALESCE(db_mgi.new_index, seq_zlims.id_index) AS id_index
				FROM zlims_samples seq_zlims
				LEFT JOIN db_mgi_fixes db_mgi
					ON seq_zlims.id_repository = db_mgi.id_repository
					AND seq_zlims.id_flowcell = db_mgi.id_library
				WHERE seq_zlims.date_create >= '2023-11-01'

				UNION ALL

				-- ztronpro_samples
				SELECT
					ROW_NUMBER() OVER (
						PARTITION BY COALESCE(db_mgi2.new_repository, REGEXP_SUBSTR(zpro.original_sample_id, "[^_]+"))
						ORDER BY zpro.creation_time DESC
					) rn
					, COALESCE(db_mgi2.new_repository, REGEXP_SUBSTR(zpro.original_sample_id, "[^_]+")) AS id_repository
					, COALESCE(db_mgi2.new_library, zpro.flow_cell_id) AS id_library
					, 'MGI' AS sequencer
					, zpro.creation_time AS date_primary
					, NULL AS sum_of_total_passed_bases
					, NULL AS sum_of_bam_size
					, COALESCE(db_mgi2.new_index, CAST(NULLIF(zpro.barcode, '') AS UNSIGNED)) AS id_index
				FROM 
					ztronpro_samples zpro
				LEFT JOIN db_mgi_fixes db_mgi2
					ON REGEXP_SUBSTR(zpro.original_sample_id, "[^_]+") = db_mgi2.id_repository
						AND zpro.flow_cell_id = db_mgi2.id_library
				WHERE NOT REGEXP_LIKE(original_sample_id, "(?i)test|AshkenazimTrio")
					AND REGEXP_SUBSTR(zpro.original_sample_id, "[^_]+") NOT IN (
						SELECT code_repository 
						FROM deleted_hg37
					)
					AND sample_progress = 'Completed'
			) temp_zlims
			WHERE rn = 1
		)

		-- Union all platforms
		SELECT
			id_repository
			, id_library
			, sequencer
			, date_primary
			, sum_of_total_passed_bases
			, sum_of_bam_size
			, id_index
		FROM (
			SELECT *, ROW_NUMBER() OVER (
				PARTITION BY id_repository
				ORDER BY date_primary DESC
			) rn
			FROM mgi
		) t
		WHERE rn = 1

		UNION ALL

		-- Illumina records
		SELECT
			COALESCE(sfki.code_repository, t.id_repository)
			, id_library
			, sequencer
			, date_primary
			, NULL
			, NULL
			, NULL
		FROM (
			SELECT
				ROW_NUMBER() OVER (
					PARTITION BY seq_ica.clean_id_repository
					ORDER BY seq_ica.time_modified DESC
				) rn
				, COALESCE(db_ica.new_repository, seq_ica.clean_id_repository) AS id_repository
				, COALESCE(db_ica.new_library, seq_ica.id_library) AS id_library
				, 'Illumina' AS sequencer
				, time_modified AS date_primary
			FROM (
				SELECT
					CASE
						WHEN id_repository LIKE '%DRAGEN%' THEN REGEXP_SUBSTR(id_repository, '[\\w\\d]+')
						WHEN id_repository LIKE '%_M' OR id_repository LIKE '%_T' THEN REGEXP_SUBSTR(id_repository, '[A-Za-z0-9]+')
						ELSE id_repository
					END AS clean_id_repository
					, NULLIF(
						COALESCE(
							REGEXP_SUBSTR(TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(sample_list_technical_tags, '''bssh.run.name:LP.* '''), "[\'\",]", "")), 'LP.+'),
							TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(tag_user_tags, '''LP.+?'''), "[\'\"]", ""))
						),
						'EMPTY'
					) AS id_library
					, time_modified
				FROM ica_samples
				LEFT JOIN ski_exclusion ski 
					ON ica_samples.id_repository = ski.origin_code_repository
				WHERE NOT REGEXP_LIKE(ica_samples.id_repository, "(?i)(demo|test|benchmark|dev)")
					AND `status` != 'DELETED'
					AND ski.origin_code_repository IS NULL
			) seq_ica

			LEFT JOIN (
			SELECT
			    id_repository
			    , NULLIF(id_library, 'EMPTY') AS id_library
			    , MAX(new_repository) AS new_repository
			    , MAX(new_library) AS new_library
			FROM (
			    SELECT
			        id_repository
			        , id_library
			        , CASE WHEN fix_type = 'id_repository' THEN new_repository END AS new_repository
			        , CASE WHEN fix_type = 'id_library' THEN new_library END AS new_library
			    FROM (
			        SELECT *,
			            ROW_NUMBER() OVER (
			                PARTITION BY id_repository, id_library, fix_type
			                ORDER BY time_requested DESC
			            ) AS rn
			        FROM dynamodb_fix_samples
			        WHERE sequencer = 'Illumina'
			    ) ranked
			    WHERE rn = 1
			) pivoted
			GROUP BY 
				id_repository
				, NULLIF(id_library, 'EMPTY')
			) db_ica
			ON seq_ica.clean_id_repository = db_ica.id_repository
			-- AND seq_ica.id_library = db_ica.id_library
		) t
		LEFT JOIN staging_fix_ski_id_repo sfki 
			ON t.id_repository = sfki.new_origin_code_repository
		WHERE rn = 1

		UNION ALL

		-- ONT (WFHV) records
		SELECT
			seq_wfhv.id_repository
			, seq_wfhv.id_batch id_library
			, 'ONT' AS sequencer
			, (
				SELECT MAX(j.ts)
				FROM JSON_TABLE(
					REPLACE(seq_wfhv.processing_stopped, '''', '"'),
					'$[*]' COLUMNS (ts DATETIME PATH '$')
				) AS j
			)
			, seq_wfhv.sum_of_total_passed_bases
			, seq_wfhv.sum_of_bam_size
			, NULL AS id_index
		FROM 
			wfhv_samples seq_wfhv
		WHERE NOT REGEXP_LIKE(seq_wfhv.id_repository, "(?i)(demo|test|benchmark|dev|barcode)")
);
COMMIT;
