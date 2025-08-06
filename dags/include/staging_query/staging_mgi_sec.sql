/*
 ---------------------------------------------------------------------------------------------------------------------------------
 -- Purpose  :   This query is intended to be used as the staging results of illumina secondary analysis data.
 -- Author   :   Abdullah Faqih
 -- Created  :   14-02-2025
 -- Changes	 :	 15-03-2025 Abdullah Faqih - Adding filter to remove test, demo, benchmark, and dev id repositories.
				 30-04-2025 Abdullah Faqih - Adding ztron pro analysis
				 30-06-2025 Abdullah Faqih - Adding transcation lock to the query
				 05-08-2025 Renata Triwijaya - Adding dynamodb-fix on MGI analysis table 				
 ---------------------------------------------------------------------------------------------------------------------------------
 */
-- Your SQL code goes here
START TRANSACTION;
DELETE FROM staging_mgi_sec;
INSERT INTO
	staging_mgi_sec (
		WITH
			dbfa_mgi_1 AS (
				SELECT 
					id_repository
					, id_library
					, new_repository
				FROM (
					SELECT
						id_repository
						, id_library
						, new_repository
						, ROW_NUMBER() OVER (
							PARTITION BY id_repository, id_library
							ORDER BY time_requested DESC
						) AS rn
					FROM dynamodb_fix_samples
					WHERE
						sequencer = 'MGI'
						AND fix_type = 'id_repository'
						AND new_repository IS NOT NULL
				) ranked
				WHERE rn = 1
			)

			, dbfa_mgi_2 AS (
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
							PARTITION BY run_name
							ORDER BY time_requested DESC
						) AS rn
					FROM dynamodb_fix_analysis
					WHERE sequencer = 'MGI'
				) ranked
				WHERE rn = 1
				GROUP BY 
					run_name
					, id_repository
			)

			, res AS (
				SELECT
					date_start
					, COALESCE(dbfa_mgi_2.new_repository,dbfa_mgi_1.new_repository,mgi_analysis_2.id_repository) id_repository
					, mgi_analysis_2.id_flowcell id_batch
					, mgi_analysis_2.pipeline_name
					, mgi_analysis_2.run_name
					, COALESCE(dbfa_mgi_2.new_cram, mgi_analysis_2.cram) cram
					, COALESCE(dbfa_mgi_2.new_cram_size, mgi_analysis_2.cram_size) cram_size
					, COALESCE(dbfa_mgi_2.new_vcf, mgi_analysis_2.vcf) vcf
					, COALESCE(dbfa_mgi_2.new_vcf_size, mgi_analysis_2.vcf_size) vcf_size
					, NULL tag_user_tags
					, mgi_qc_2.percent_dups
					, NULL percent_q30_bases
					, mgi_qc_2.total_seqs
					, mgi_qc_2.median_coverage
					, NULL contamination
					, mgi_qc_2.at_least_10x
					, mgi_qc_2.at_least_20x
					-- Ploidy estimation of MGI are in "female" and "male" or "nan"
					, CASE
						WHEN LOWER(mgi_qc_2.ploidy_estimation) = "female" THEN "XX"
						WHEN LOWER(mgi_qc_2.ploidy_estimation) = "male" THEN "XY"
						ELSE NULL
					END ploidy_estimation
					, mgi_qc_2.snp
					, mgi_qc_2.indel
					, mgi_qc_2.ts_tv
				FROM
					(
						SELECT
							-- Several id repositories were run more than once (expected). 
							-- There isn't any other information available other than date_start
							*
							, ROW_NUMBER() OVER (
								PARTITION BY
									id_repository,
									run_name
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
							*
							, ROW_NUMBER() OVER (
								PARTITION BY
									id_repository
								ORDER BY
									at_least_50x DESC
							) AS rn
							-- Get the best q30 assuming that the biggest one is the latest run.
							-- HOWEVER THIS LOGIC HAS FLAW! -> i.e. the same id_repo with different run_name can have different quality 
							-- unless we partitino only by an id_repo and order by the date DEC, we can have consistent data. 
						FROM
							mgi_qc
						WHERE
							NOT REGEXP_LIKE(id_repository, "(?i)(demo|test|benchmark|dev)")
					) mgi_qc_2
						-- ON mgi_analysis_2.run_name = mgi_qc_2.run_name
						ON mgi_analysis_2.id_repository = mgi_qc_2.id_repository
						AND mgi_qc_2.rn = 1
					LEFT JOIN dbfa_mgi_1
						ON mgi_analysis_2.id_repository = dbfa_mgi_1.id_repository
					LEFT JOIN dbfa_mgi_2 
						ON mgi_analysis_2.run_name = dbfa_mgi_2.run_name
				WHERE
					mgi_analysis_2.rn = 1

				UNION ALL
				
				SELECT
					ztronpro_analysis.date_secondary date_start
					, COALESCE(dbfa_mgi_1.new_repository, ztronpro_analysis.id_repository) id_repository
					, NULL id_batch
					, "ZTRONPRO-MEGABOLT" pipeline_name
					, ztronpro_analysis.run_name
					, COALESCE(dbfa_mgi_2.cram, ztronpro_analysis.cram) cram
					, COALESCE(dbfa_mgi_2.new_cram_size, ztronpro_analysis.cram_size) cram_size
					, COALESCE(dbfa_mgi_2.vcf, ztronpro_analysis.vcf) vcf
					, COALESCE(dbfa_mgi_2.vcf_size, ztronpro_analysis.vcf_size) vcf_size
					, NULL tag_user_tags
					, ztronpro_qc.percent_dups
					, NULL percent_q30_bases
					, NULL total_seqs
					, ztronpro_qc.depth median_coverage
					, NULL contamination
					, ztronpro_qc.at_least_10x
					, ztronpro_qc.at_least_20x
					-- Ploidy estimation of MGI are in "female" and "male" or "nan"
					, CASE
						WHEN LOWER(ploidy_estimation) = "female" THEN "XX"
						WHEN LOWER(ploidy_estimation) = "male" THEN "XY"
						ELSE NULL
					END ploidy_estimation
					, snp
					, indel
					, ts_tv
				FROM
					ztronpro_analysis
				LEFT JOIN 
					ztronpro_qc
					ON ztronpro_analysis.id_repository = ztronpro_qc.id_repository 
						AND ztronpro_analysis.run_name = ztronpro_qc.run_name
				LEFT JOIN
					dbfa_mgi_1
						ON ztronpro_analysis.id_repository = dbfa_mgi_1.id_repository
				LEFT JOIN 
					dbfa_mgi_2
						ON ztronpro_analysis.run_name = dbfa_mgi_2.run_name
			)
		SELECT
			date_start
			, id_repository
			, id_batch
			, pipeline_name
			, run_name
			, cram
			, cram_size
			, vcf
			, vcf_size
			, tag_user_tags
			, percent_dups
			, percent_q30_bases
			, total_seqs
			, median_coverage
			, contamination
			, at_least_10x
			, at_least_20x
			, ploidy_estimation
			, snp
			, indel
			, ts_tv
		FROM
			(
				SELECT
					*,
					ROW_NUMBER() OVER (
						PARTITION BY
							id_repository
						ORDER BY
							date_start DESC
					) rn
				FROM
					res
			) t
		WHERE
			rn = 1
	);
COMMIT;