DELETE FROM staging_illumina_sec;
INSERT INTO staging_illumina_sec(
SELECT
	date_start,
	COALESCE(db_ic_sec.new_repository, ica_sec.id_repository) id_repository,
	ica_sec.new_id_batch,
	ica_sec.pipeline_name,
	ica_sec.run_name,
	ica_sec.cram,
	ica_sec.cram_size,
	ica_sec.vcf,
	ica_sec.vcf_size,
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
					id_repository
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
					TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(tag_user_tags, '''LP.+?'''), "[\'\"]", "")) new_id_batch
				FROM
					ica_analysis
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
					id_repository
				ORDER BY
					percent_q30_bases DESC
			) AS rn
		FROM
			illumina_qc
	) illumina_qc_2 ON ica_sec.id_repository = illumina_qc_2.id_repository
	AND illumina_qc_2.rn = 1
	LEFT JOIN (
		SELECT
			id_repository,
			SUM(yield) yield,
			SUM(yield_q30) yield_q30
		FROM
			illumina_qs
		GROUP BY
			id_repository
	) illumina_qs_2 ON ica_sec.id_repository = illumina_qs_2.id_repository
	# So TYPE TURNED OUT TO BE FOUND WITHIN TWO PLACES: SAMPLES AND THE ANALYSIS.
	LEFT JOIN (
		SELECT DISTINCT
			id_repository,
			new_repository
		FROM
			dynamodb_fix_id_repository_latest
		WHERE
			sequencer = "Illumina"
	) db_ic_sec ON ica_sec.id_repository = db_ic_sec.id_repository
WHERE
	ica_sec.rn = 1)