/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as the source of QC metrics.
-- Author   :   Abdullah Faqih
-- Created  :   16-02-2025
-- Changes	: 
				- 24-02-2025: Adding new columns for QC categories
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here 
DROP TABLE IF EXISTS gold_qc;

CREATE TABLE gold_qc
(
WITH cte AS (
    SELECT *,
        CASE
        	-- previously I set id_library instead getting signficantly lower number. Changed to id_batch instead.
            WHEN COUNT(CASE WHEN sex_ploidy_category = 'Mismatch' THEN 1 END) 
                OVER (PARTITION BY id_library) > 0 THEN 'Fail'
            WHEN COUNT(CASE WHEN sex_ploidy_category = 'No Data' THEN 1 END) 
                OVER (PARTITION BY id_library) > 0 THEN 'Incomplete Data'
            ELSE 'Pass'
        END AS batch_sex_category
    FROM (		SELECT
			seq.id_repository,
			sbp.id_patient,
			sbp.id_mpi,
			sbp.id_subject,
		    sbp.biobank_nama origin_biobank,
			# null sex means we can't find the simbiox data on both registries.
			sbp.registry_sex sex,
			seq.date_primary,
			-- seq.id_library,
			seq.sequencer,
			seq.id_index id_index_zlims,
		    -- COALESCE(seq.id_library, illumina_sec.id_batch, mgi_sec.id_batch) AS id_batch,
			-- MGI and ONT SHOULD CONTAIN id_library on samples. Illumina not consistent
			-- EMPTRY STRING IS NOT NULL!!!!!
			COALESCE(NULLIF(seq.id_library, ''), illumina_sec.id_batch) AS id_library,
			-- seq.id_library for ONT id_batch (secondary)
			COALESCE(NULLIF(illumina_sec.id_batch, ''), NULLIF(mgi_sec.id_batch, ''), seq.id_library) AS id_batch,
			seq.sum_of_total_passed_bases,
			seq.sum_of_bam_size,
		    COALESCE(mgi_sec.date_start, illumina_sec.date_start, ont_sec.date_start) date_secondary,
		    COALESCE(mgi_sec.run_name, illumina_sec.run_name, ont_sec.run_name) run_name,
		    COALESCE(mgi_sec.pipeline_name, illumina_sec.pipeline_name, ont_sec.pipeline_name) pipeline_name,
			COALESCE(mgi_sec.cram, illumina_sec.cram, ont_sec.cram) AS cram,
			COALESCE(mgi_sec.cram_size, illumina_sec.cram_size, ont_sec.cram_size) AS cram_size,
		    COALESCE(mgi_sec.vcf, illumina_sec.vcf, ont_sec.vcf) vcf,
		    COALESCE(mgi_sec.vcf_size, illumina_sec.vcf_size, ont_sec.vcf_size) vcf_size,
		    -- QC   
			COALESCE(mgi_sec.at_least_10x, illumina_sec.at_least_10x, ont_sec.at_least_10x) AS at_least_10x,
			COALESCE(mgi_sec.at_least_20x, illumina_sec.at_least_20x, ont_sec.at_least_20x) AS at_least_20x,
		    COALESCE(mgi_sec.median_coverage, illumina_sec.median_coverage, ont_sec.median_coverage) AS coverage,
			-- mgi doesnt contain contamination data
			COALESCE(illumina_sec.contamination, ont_sec.contamination) AS contamination,
			-- only illumina contain this metric
			illumina_sec.percent_q30_bases,
			-- ont doesn't produce yield metric
			COALESCE(illumina_sec.yield, ont_sec.yield) AS yield,
			-- only illumina contain this 
			illumina_sec.yield_q30,
			UPPER(COALESCE(mgi_sec.ploidy_estimation, illumina_sec.ploidy_estimation, ont_sec.ploidy_estimation)) ploidy_estimation,
			CASE
				WHEN UPPER(COALESCE(mgi_sec.ploidy_estimation, illumina_sec.ploidy_estimation, ont_sec.ploidy_estimation)) = 'XX' AND UPPER(COALESCE(sbp.registry_sex)) = 'FEMALE' THEN 'Match'
				WHEN UPPER(COALESCE(mgi_sec.ploidy_estimation, illumina_sec.ploidy_estimation, ont_sec.ploidy_estimation)) = 'XY' AND UPPER(COALESCE(sbp.registry_sex)) = 'MALE' THEN 'Match'
				WHEN COALESCE(sbp.registry_sex) IS NULL THEN 'No Data'
				WHEN COALESCE(mgi_sec.ploidy_estimation, illumina_sec.ploidy_estimation, ont_sec.ploidy_estimation) IS NULL OR COALESCE(mgi_sec.ploidy_estimation, illumina_sec.ploidy_estimation, ont_sec.ploidy_estimation) = 'nan' OR  COALESCE(mgi_sec.ploidy_estimation, illumina_sec.ploidy_estimation, ont_sec.ploidy_estimation) = '' THEN 'No Data'
				ELSE 'Mismatch'
			END sex_ploidy_category
		FROM
			staging_seq seq
			LEFT JOIN staging_simbiox_biosamples_patients sbp ON seq.id_repository = sbp.code_repository
			# Started from here, the number of rows can be duplicated i.e. an id_repository can have multiple secondary analysis run with different run_name.
			# Unless we do window functino on id_repo ONLY based on datetime available.
			LEFT JOIN
				# Since now the staing contains rerun data. Need to dedup by latest date_start run
				(
					SELECT
						*
					FROM
						(
							SELECT
								ROW_NUMBER() OVER (
									PARTITION BY
										id_repository
									ORDER BY
										date_start DESC
								) rn,
								staging_illumina_sec.*
							FROM
								staging_illumina_sec
						) t1
					WHERE
						t1.rn = 1
				) illumina_sec
			ON (seq.id_repository = illumina_sec.id_repository AND seq.sequencer="Illumina")
			LEFT JOIN
				# Since now the staing contains rerun data. Need to dedup by latest date_start run
				(
					SELECT
						*
					FROM
						(
							SELECT
								ROW_NUMBER() OVER (
									PARTITION BY
										id_repository
									ORDER BY
										date_start DESC
								) rn,
								staging_mgi_sec.*
							FROM
								staging_mgi_sec
						) t1
					WHERE
						t1.rn = 1
				) mgi_sec
			ON (seq.id_repository = mgi_sec.id_repository AND seq.sequencer="MGI")
			LEFT JOIN 
				# Since now the staing contains rerun data. Need to dedup by latest date_start run
				(
					SELECT
						*
					FROM
						(
							SELECT
								ROW_NUMBER() OVER (
									PARTITION BY
										id_repository
									ORDER BY
										date_start DESC
								) rn,
								staging_ont_sec.*
							FROM
								staging_ont_sec
						) t1
					WHERE
						t1.rn = 1
				) 
			 ont_sec
			ON (seq.id_repository = ont_sec.id_repository AND seq.sequencer="ONT")
        WHERE seq.sequencer IS NOT NULL
	        	AND LOWER(seq.id_repository) NOT LIKE '%test%'
	            AND LOWER(seq.id_repository) NOT LIKE '%pro%'
	            AND LOWER(seq.id_repository) NOT LIKE '%mla%'
	            AND LOWER(seq.id_repository) NOT LIKE '%sp500%'
	            AND LOWER(seq.id_repository) NOT LIKE '%gk-p5%'
	            AND LOWER(seq.id_repository) NOT LIKE '%gk-p3%'
	            AND LOWER(seq.id_repository) NOT LIKE '%control%'
	            AND LOWER(seq.id_repository) NOT LIKE '%T011%'
    ) AS subquery
) 

SELECT 
	*,
	CASE
		WHEN coverage >= 30 AND at_least_10x >= 90 THEN 
			CASE 
				WHEN batch_sex_category = 'Pass' THEN 'Pass'
				WHEN batch_sex_category = 'Incomplete Data' THEN 'No Data'
				ELSE 'Fail'
			END
		WHEN (coverage < 30 OR at_least_10x < 90) AND coverage IS NOT NULL AND at_least_10x IS NOT NULL THEN 'Fail'
		WHEN batch_sex_category ='Fail' THEN 'Fail'
		WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NOT NULL THEN 'No Data'
		WHEN sex IS NULL AND (coverage IS NOT NULL AND at_least_10x IS NOT NULL) AND (batch_sex_category <> 'Fail' OR batch_sex_category IS NULL) THEN 'No Data'
		WHEN sex IS NULL AND coverage IS NULL AND at_least_10x IS NULL THEN 'No Data'
		ELSE 'Undefined'
	END AS qc_category,

	CASE
		WHEN coverage >= 30 AND at_least_10x >= 90 THEN 
			CASE 
				WHEN batch_sex_category = 'Pass' THEN 'Pass'
				WHEN batch_sex_category = 'Incomplete Data' AND ploidy_estimation IS NOT NULL AND sex IS NULL THEN 'No Registry Data'
				WHEN batch_sex_category = 'Incomplete Data' AND ploidy_estimation IS NULL AND sex IS NULL THEN 'No Data'
				WHEN batch_sex_category = 'Incomplete Data' AND sex IS NOT NULL THEN 'In Progress Analysis'
				ELSE 'Fail'
			END
		WHEN (coverage < 30 OR at_least_10x < 90) AND coverage IS NOT NULL AND at_least_10x IS NOT NULL THEN 'Fail'
		WHEN batch_sex_category ='Fail' THEN 'Fail'
		WHEN sex IS NULL AND (coverage IS NULL OR at_least_10x IS NULL OR ploidy_estimation IS NULL) THEN 'No Data'
		WHEN (coverage IS NULL OR at_least_10x IS NULL OR ploidy_estimation IS NULL) AND sex IS NOT NULL THEN 'In Progress Analysis'
		WHEN sex IS NULL AND (coverage IS NOT NULL AND at_least_10x IS NOT NULL) AND (batch_sex_category <> 'Fail' OR batch_sex_category IS NULL) THEN 'No Registry Data'

		ELSE 'Undefined'
	END qc_category2,
	
	CASE 
		WHEN (coverage >= 30 AND at_least_10x >= 90) THEN 
			CASE 
				WHEN batch_sex_category = 'Pass' THEN 'Pass'
				WHEN sex_ploidy_category = 'Match' AND batch_sex_category = 'Fail' THEN 'Blacklisted'
				WHEN sex_ploidy_category = 'Mismatch' AND batch_sex_category = 'Fail' THEN 'Blacklisted'
				WHEN batch_sex_category = 'Fail' THEN
					CASE
						WHEN sex IS NULL AND ploidy_estimation IS NULL THEN 'Blacklisted'
						WHEN sex IS NULL AND ploidy_estimation IS NOT NULL THEN 'Blacklisted'
						ELSE 'Blacklisted'
					END
				WHEN batch_sex_category = 'Incomplete Data' AND sex_ploidy_category = 'Match' THEN 'Graylisted'
				WHEN sex IS NULL AND ploidy_estimation IS NULL THEN 'Graylisted'
				WHEN sex IS NULL AND ploidy_estimation IS NOT NULL THEN 'Graylisted'
				ELSE 'Graylisted'
			END

		WHEN sequencer ='ONT' AND sum_of_total_passed_bases < 90000000000 THEN 'Top Up List'

		WHEN coverage < 30 OR at_least_10x < 90 THEN 
			CASE 
				WHEN batch_sex_category = 'Pass' THEN 'Top Up List'

				WHEN sex_ploidy_category = 'Mismatch' THEN 'Blacklisted'

				WHEN batch_sex_category = 'Fail' THEN 
					CASE 
						WHEN ploidy_estimation IS NULL AND sex IS NULL THEN 
							'Blacklisted'

						WHEN sex IS NULL THEN 
							'Blacklisted'

						WHEN sex_ploidy_category = 'Match' THEN 
							'Blacklisted'

						ELSE 'Blacklisted'
					END

				WHEN batch_sex_category = 'Incomplete Data' THEN 
					CASE
						WHEN sex_ploidy_category = 'Match' THEN 'Graylisted'
						WHEN sex IS NULL AND ploidy_estimation IS NULL THEN 'Graylisted'
						WHEN ploidy_estimation IS NULL THEN 'Graylisted'
						Else 'Graylisted'
					END

				WHEN ploidy_estimation IS NULL AND sex IS NULL THEN 'Graylisted'
				WHEN ploidy_estimation IS NULL THEN 'Graylisted'
				WHEN sex IS NULL THEN 'Graylisted'

				ELSE 'Blacklisted'
			END 

		WHEN batch_sex_category = 'Fail' THEN 
			CASE 
				WHEN sex_ploidy_category = 'Match' THEN 'test'
				WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NOT NULL AND ploidy_estimation IS NULL THEN 'Blacklisted'
				WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NULL AND ploidy_estimation IS NOT NULL THEN 'Blacklisted'
				WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NULL AND ploidy_estimation IS NULL THEN 'Blacklisted'
				ELSE 'Blacklisted'
			END

		WHEN coverage IS NOT NULL AND at_least_10x IS NOT NULL AND batch_sex_category <> 'Fail' THEN 
			CASE 
				WHEN sex IS NULL AND ploidy_estimation IS NOT NULL THEN 'Graylisted'
				WHEN sex IS NOT NULL AND ploidy_estimation IS NULL THEN 'Graylisted'
				WHEN sex IS NULL AND ploidy_estimation IS NULL THEN 'Graylisted'
			END

		WHEN sex IS NOT NULL AND ploidy_estimation IS NOT NULL AND (coverage IS NULL OR at_least_10x IS NULL) THEN 'Graylisted'

		WHEN sex IS NOT NULL AND ploidy_estimation IS NULL AND (coverage IS NULL OR at_least_10x IS NULL) THEN 'Graylisted'

		WHEN sex IS NULL AND ploidy_estimation IS NOT NULL AND (coverage IS NULL OR at_least_10x IS NULL) THEN 'Graylisted'

		WHEN sex IS NULL AND ploidy_estimation IS NULL AND coverage IS NULL AND at_least_10x IS NULL THEN 'Graylisted'

		ELSE 'Unidentified'
	END AS qc_category3,

	CASE 
		WHEN (coverage >= 30 AND at_least_10x >= 90) THEN 
			CASE 
				WHEN batch_sex_category = 'Pass' THEN 'Pass'
				WHEN sex_ploidy_category = 'Match' AND batch_sex_category = 'Fail' THEN 'Fail (Batch Sex Check)'
				WHEN sex_ploidy_category = 'Mismatch' AND batch_sex_category = 'Fail' THEN 'Fail (Sex Check)'
				WHEN batch_sex_category = 'Fail' THEN
					CASE
						WHEN sex IS NULL AND ploidy_estimation IS NULL THEN 'Fail (Batch Sex QC), No Data (No Sex Data)'
						WHEN sex IS NULL AND ploidy_estimation IS NOT NULL THEN 'Fail (Batch Sex Check), No Data (No Registry Sex Data)'
						ELSE 'Fail (Batch Sex Check), No Data (No Ploidy Sex Data)'
					END
				WHEN batch_sex_category = 'Incomplete Data' AND sex_ploidy_category = 'Match' THEN 'No Data (Incomplete Batch Sex Data)'
				WHEN sex IS NULL AND ploidy_estimation IS NULL THEN 'No Data (No Sex Data)'
				WHEN sex IS NULL AND ploidy_estimation IS NOT NULL THEN 'No Data (No Registry Sex Data)'
				ELSE 'No Data (No Ploidy Data)'
			END

		WHEN coverage < 30 OR at_least_10x < 90 THEN 
			CASE 
				WHEN batch_sex_category = 'Pass' THEN 'Fail (Coverage QC)'

				WHEN sex_ploidy_category = 'Mismatch' THEN 'Fail (Coverage and Sex Check)'

				WHEN batch_sex_category = 'Fail' THEN 
					CASE 
						WHEN ploidy_estimation IS NULL AND sex IS NULL THEN 
							'Fail (Coverage and Batch Sex Check), No Data (No Sex Data)'

						WHEN sex IS NULL THEN 
							'Fail (Coverage and Batch Sex Check), No Data (No Registry Sex Data)'

						WHEN sex_ploidy_category = 'Match' THEN 
							'Fail (Coverage and Batch Sex Check)'

						ELSE 'Fail (Coverage and Batch Sex Check), No Data (No Ploidy Data)'
					END

				WHEN batch_sex_category = 'Incomplete Data' THEN 
					CASE
						WHEN sex_ploidy_category = 'Match' THEN 'Fail (Coverage QC), No Data (Incomplete Batch Sex check)'
						WHEN sex IS NULL AND ploidy_estimation IS NULL THEN 'Fail (Coverage QC), No Data (No Sex Data)'
						WHEN ploidy_estimation IS NULL THEN 'Fail (Coverage QC), No Data (No Ploidy Sex Data)'
						Else 'Fail (Coverage QC), No Data (No Registry Data)'
					END

				-- If ploidy estimation or sex data is missing, categorize the failure accordingly
				WHEN ploidy_estimation IS NULL AND sex IS NULL THEN 'Fail (Coverage QC), No Data (No Sex Data)'
				WHEN ploidy_estimation IS NULL THEN 'Fail (Coverage QC), No Data (No Ploidy Data)'
				WHEN sex IS NULL THEN 'Fail (Coverage QC), No Data (No Registry Sex Data)'

				ELSE 'Fail (Coverage and Sex QC)'
			END 

		WHEN batch_sex_category = 'Fail' THEN 
			CASE 
				WHEN sex_ploidy_category = 'Match' THEN 'test'
				WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NOT NULL AND ploidy_estimation IS NULL THEN 'Fail (Batch Sex Check), No Data (No Coverage and Ploidy Data)'
				WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NULL AND ploidy_estimation IS NOT NULL THEN 'Fail (Batch Sex Check), No Data (No Coverage and Sex Registry Data)'
				WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NULL AND ploidy_estimation IS NULL THEN 'Fail (Batch Sex Check), No Data (No Coverage and Sex Data)'
				ELSE 'Fail (Batch Sex Check)'
			END

		WHEN coverage IS NOT NULL AND at_least_10x IS NOT NULL AND batch_sex_category <> 'Fail' THEN 
			CASE 
				WHEN sex IS NULL AND ploidy_estimation IS NOT NULL THEN 'No Data (No Registry Sex Data)'
				WHEN sex IS NOT NULL AND ploidy_estimation IS NULL THEN 'No Data (No Ploidy Data)'
				WHEN sex IS NULL AND ploidy_estimation IS NULL THEN 'No Data (No Sex Data)'
			END

		WHEN sex IS NOT NULL AND ploidy_estimation IS NOT NULL AND (coverage IS NULL OR at_least_10x IS NULL) THEN 'No Coverage Data'

		WHEN sex IS NOT NULL AND ploidy_estimation IS NULL AND (coverage IS NULL OR at_least_10x IS NULL) THEN 'No QC Data'

		WHEN sex IS NULL AND ploidy_estimation IS NOT NULL AND (coverage IS NULL OR at_least_10x IS NULL) THEN 'No Registry Sex and Coverage Data'

		WHEN sex IS NULL AND ploidy_estimation IS NULL AND coverage IS NULL AND at_least_10x IS NULL THEN 'No Data'

		-- Default Case for Any Other Failure
		ELSE 'Unidentified'
	END AS qc_category4,

	CASE 
	    WHEN coverage IS NULL OR at_least_10x IS NULL THEN 'No Data'
	    WHEN coverage >= 30 AND at_least_10x >= 90 THEN 'Pass'
	    ELSE 'Fail'
	END AS coverage_category,
	CURRENT_TIMESTAMP updated_at
FROM 
	cte
);