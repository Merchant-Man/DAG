/*
 ---------------------------------------------------------------------------------------------------------------------------------
 -- Purpose  :   This query is intended to be used as the staging results of illumina secondary analysis data.
 -- Author   :   Abdullah Faqih
 -- Created  :   14-02-2025
 -- Changes	 :	 01-03-2025 Abdullah Faqih - Enforce the SKI code repo into the new id repo. SKI should be the origin_code_repo not the id_repo itself. 
				 15-03-2025 Abdullah Faqih - Adding filter to remove test, demo, benchmark, and dev id repositories.
				 30-06-2025 Abdullah Faqih - Adding transcation lock to the query
				 04-08-2025 Renata Triwijaya - Adding dynamodb-fix on Illumina analysis table
 ---------------------------------------------------------------------------------------------------------------------------------
 */
-- Your SQL code goes here

START TRANSACTION;
DELETE FROM staging_illumina_sec;
INSERT INTO staging_illumina_sec
( SELECT
	date_start,
	COALESCE(sfki.code_repository, COALESCE(db_ic_sec.new_repository,db_ic_sec2.new_repository,ica_sec.clean_id_repository)) id_repository,
	ica_sec.new_id_batch id_batch,
	ica_sec.pipeline_name,
	ica_sec.run_name,
	COALESCE(db_ic_sec2.new_cram, ica_sec.cram) cram,
	COALESCE(db_ic_sec2.new_cram_size, ica_sec.cram_size) cram_size,
	COALESCE(db_ic_sec2.new_vcf, ica_sec.vcf) vcf,
	COALESCE(db_ic_sec2.new_vcf_size,ica_sec.vcf_size) vcf_size,
	ica_sec.tag_user_tags,
	illumina_qc_2.percent_dups,
	illumina_qc_2.percent_q30_bases,
	illumina_qc_2.total_seqs,
	illumina_qc_2.median_coverage,
	illumina_qc_2.contam contamination,
	illumina_qc_2.at_least_10x,
	illumina_qc_2.at_least_20x,
	illumina_qc_2.ploidy_estimation,
	illumina_qc_2.snp,
	illumina_qc_2.indel,
	illumina_qc_2.ts_tv,
	illumina_qs_2.yield,
	illumina_qs_2.yield_q30
FROM
	(
		SELECT
			-- id_repo 0G0048601C01 and  run_name 0G0048601C01_63dbb038 has two records with almost the same columns' values except the cram file
			-- we assume that the last time_modified column as the chosen analysis.
			*,
			-- ROW_NUMBER() OVER ( PARTITION BY id_repository, id_batch, run_name ORDER BY date_end DESC) AS rn # Get the latest succeded run by rn=1
			-- !!!!!!!!!!!!! NOTE !!!!!!!!!!!!!
			-- Here I chose to only include the latest run for an id_repository assuming that that latest run will be used for the downstream analysis.
			-- This decision causing the number of row is consistent.
			ROW_NUMBER() OVER (
				PARTITION BY
					clean_id_repository, run_name
				ORDER BY
					date_end DESC
			) AS rn
		FROM
			(
			--     df["id_batch"] = df["tags"].apply(lambda x: ast.literal_eval(
			--         x)["userTags"][4] if len(ast.literal_eval(x)["userTags"]) > 4 else None)
			-- If the length of the tags less than or equal to 4, the above script is not valid. 
			-- this is alsoe the case, when the location of tag is not in the fifth position.
			-- It is way better to use regex to get the id_batch!!!!!!
				SELECT
					*,
					TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(tag_user_tags, '''LP.+?'''), "[\'\"]", "")) new_id_batch,
					CASE
					-- DRAGEN
						WHEN id_repository LIKE "%DRAGEN%" THEN REGEXP_SUBSTR(id_repository, "[\\w\\d]+")
						-- TOP UP 
						WHEN id_repository LIKE "%_M"  OR id_repository LIKE "%_T" THEN REGEXP_SUBSTR(id_repository, "[A-Za-z0-9]+")
						ELSE id_repository
					END clean_id_repository
				FROM
					ica_analysis
				WHERE NOT REGEXP_LIKE(id_repository, "(?i)(demo|test|benchmark|dev)")
			) t
		WHERE
			run_status = "SUCCEEDED"
	) ica_sec
	LEFT JOIN (
		SELECT
			# Get the best q30 assuming that the biggest one is the latest run
			*,
			ROW_NUMBER() OVER (
				PARTITION BY
					clean_id_repository, run_name
				ORDER BY
					percent_q30_bases DESC
			) AS rn
		FROM
			(
			SELECT
				*,
			CASE
			-- DRAGEN
				WHEN id_repository LIKE "%DRAGEN%" THEN REGEXP_SUBSTR(id_repository, "[\\w\\d]+")
				-- TOP UP 
				WHEN id_repository LIKE "%_M"  OR id_repository LIKE "%_T" THEN REGEXP_SUBSTR(id_repository, "[A-Za-z0-9]+")
				ELSE id_repository
			END clean_id_repository
			FROM
				illumina_qc
			WHERE NOT REGEXP_LIKE(id_repository, "(?i)(demo|test|benchmark|dev)")
			) t
	) illumina_qc_2 ON ica_sec.run_name = illumina_qc_2.run_name
	AND illumina_qc_2.rn = 1
	LEFT JOIN (
		SELECT
			id_repository,
			SUM(yield) yield,
			SUM(yield_q30) yield_q30
		FROM
			illumina_qs
		WHERE NOT REGEXP_LIKE(id_repository, "(?i)(demo|test|benchmark|dev)")
		GROUP BY
			id_repository
	) illumina_qs_2 ON ica_sec.id_repository = illumina_qs_2.id_repository
	-- 	 So TYPE TURNED OUT TO BE FOUND WITHIN TWO PLACES: SAMPLES AND THE ANALYSIS.
	LEFT JOIN (
		SELECT 
			id_repository
			, new_repository
		FROM (
			SELECT
				id_repository
				, new_repository
				, ROW_NUMBER() OVER (
					PARTITION BY id_repository
					ORDER BY time_requested DESC
				) AS rn
			FROM dynamodb_fix_samples
			WHERE
				sequencer = 'Illumina'
				AND fix_type = 'id_repository'
				AND new_repository IS NOT NULL
		) ranked
		WHERE rn = 1
	) db_ic_sec 
		ON ica_sec.id_repository = db_ic_sec.id_repository
	LEFT JOIN (
		SELECT
			run_name
			, id_repository
			, MAX(CASE WHEN fix_type = 'id_repository' THEN new_repository END) AS new_repository
			, MAX(CASE WHEN fix_type = 'cram' THEN cram END) AS cram
			, MAX(CASE WHEN fix_type = 'cram' THEN new_cram END) AS new_cram
			, MAX(CASE WHEN fix_type = 'cram' THEN cram_size END) AS cram_size
			, MAX(CASE WHEN fix_type = 'cram' THEN new_cram_size END) AS new_cram_size
			, MAX(CASE WHEN fix_type = 'vcf' THEN vcf END) AS vcf
			, MAX(CASE WHEN fix_type = 'vcf' THEN new_vcf END) AS new_vcf
			, MAX(CASE WHEN fix_type = 'vcf' THEN vcf_size END) AS vcf_size
			, MAX(CASE WHEN fix_type = 'vcf' THEN new_vcf_size END) AS new_vcf_size
		FROM (
			SELECT *,
				ROW_NUMBER() OVER (
					PARTITION BY run_name, fix_type
					ORDER BY time_requested DESC
				) AS rn
			FROM dynamodb_fix_analysis
			WHERE sequencer = 'Illumina'
		) ranked
		WHERE rn = 1
		GROUP BY run_name
	) db_ic_sec2 
		ON ica_sec.run_name = db_ic_sec2.run_name
	LEFT JOIN staging_fix_ski_id_repo sfki ON ica_sec.id_repository = sfki.new_origin_code_repository
WHERE
	ica_sec.rn = 1);
COMMIT;