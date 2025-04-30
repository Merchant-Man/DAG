/*
 ---------------------------------------------------------------------------------------------------------------------------------
 -- Purpose  :   This query is intended to be used as the staging results of illumina secondary analysis data.
 -- Author   :   Abdullah Faqih
 -- Created  :   14-02-2025
 -- Changes	 :	 15-03-2025 Adding filter to remove test, demo, benchmark, and dev id repositories.
				 30-04-2025 Adding ztron pro analysis
 ---------------------------------------------------------------------------------------------------------------------------------
 */
-- Your SQL code goes here
DELETE FROM staging_mgi_sec;
INSERT INTO staging_mgi_sec(
		SELECT
		date_start,
		COALESCE(db_mgi.new_repository, mgi_analysis_2.id_repository) id_repository,
		mgi_analysis_2.id_flowcell id_batch,
		mgi_analysis_2.pipeline_name,
		mgi_analysis_2.run_name,
		mgi_analysis_2.cram,
		mgi_analysis_2.cram_size,
		mgi_analysis_2.vcf,
		mgi_analysis_2.vcf_size,
		NULL tag_user_tags,
		mgi_qc_2.percent_dups,
		NULL percent_q30_bases,
		mgi_qc_2.total_seqs,
		mgi_qc_2.median_coverage,
		NULL contamination,
		mgi_qc_2.at_least_10x,
		mgi_qc_2.at_least_20x,
		# Ploidy estimation of MGI are in "female" and "male" or "nan"
		CASE
			WHEN LOWER(mgi_qc_2.ploidy_estimation) = "female" THEN "XX"
			WHEN LOWER(mgi_qc_2.ploidy_estimation) = "male" THEN "XY"
			ELSE NULL
		END ploidy_estimation,
		mgi_qc_2.snp,
		mgi_qc_2.indel,
		mgi_qc_2.ts_tv
	FROM
		(
			SELECT
				# Several id repositories were run more than once (expected). 
				# There isn't any other information available other than date_start
				*,
				ROW_NUMBER() OVER (
					PARTITION BY
						id_repository, run_name
					ORDER BY
						date_start DESC
				) rn
			FROM
				mgi_analysis
			WHERE
				run_status = "SUCCEEDED"
				AND NOT REGEXP_LIKE(id_repository, "(?i)(demo|test|benchmark|dev)")
		) mgi_analysis_2
		LEFT JOIN (
			SELECT
				*,
				ROW_NUMBER() OVER (
					PARTITION BY
						id_repository
					ORDER BY
						at_least_50x DESC
				) AS rn
				# Get the best q30 assuming that the biggest one is the latest run.
				# HOWEVER THIS LOGIC HAS FLAW! -> i.e. the same id_repo with different run_name can have different quality 
				# unless we partitino only by an id_repo and order by the date DEC, we can have consistent data. 
			FROM
				mgi_qc
			WHERE NOT REGEXP_LIKE(id_repository, "(?i)(demo|test|benchmark|dev)")
		) mgi_qc_2 
		-- ON mgi_analysis_2.run_name = mgi_qc_2.run_name
		ON mgi_analysis_2.id_repository = mgi_qc_2.id_repository
		AND mgi_qc_2.rn = 1
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
		) db_mgi ON mgi_analysis_2.id_repository = db_mgi.id_repository
	WHERE
		mgi_analysis_2.rn = 1

	UNION ALL 

		SELECT
		data_creation_date date_start,
		id_repository,
		NULL id_batch,
		"ZTRONPRO-MEGABOLT" pipeline_name,
		run_folder_name run_name,
		NULL cram,
		NULL cram_size,
		NULL vcf,
		NULL vcf_size,
		NULL tag_user_tags,
		percent_dups,
		NULL percent_q30_bases,
		NULL total_seqs,
		NULL median_coverage,
		NULL contamination,
		at_least_10x,
		at_least_20x,
		# Ploidy estimation of MGI are in "female" and "male" or "nan"
		CASE
			WHEN LOWER(predicted_sex) = "female" THEN "XX"
			WHEN LOWER(predicted_sex) = "male" THEN "XY"
			ELSE NULL
		END ploidy_estimation,
		snp,
		indel,
		ts_tv
	FROM 
		ztronpro_analysis	
)