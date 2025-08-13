/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as the source of QC metrics.
-- Author   :   Abdullah Faqih
-- Created  :   16-02-2025
-- Changes	: 
				02-03-2025 Abdullah Faqih - Adding filter for testing id repositories
				30-06-2025 Abdullah Faqih - Adding transcation lock to the query
				05-08-2025 Renata Triwijaya - Adding dynamodb-fix on MGI analysis table
				13-08-2025 Abdullah Faqih - Excluding test id repositories
---------------------------------------------------------------------------------------------------------------------------------
*/
START TRANSACTION;
DELETE FROM staging_ont_sec;
INSERT INTO staging_ont_sec
-- TODO - In the future it might be that we would have an id repo to be rerun with different run_name. Latest run_time should be chosen.
	(
	SELECT
	date_start
	, COALESCE(dbfa_ont.new_repository, wfhv_sec.id_repository) id_repository
	, NULL id_batch
	, wfhv_sec.pipeline_name
	, wfhv_sec.run_name
	, COALESCE(dbfa_ont.new_cram, wfhv_sec.cram) cram
	, COALESCE(dbfa_ont.new_cram_size, wfhv_sec.cram_size) cram_size
	, COALESCE(dbfa_ont.new_vcf, wfhv_sec.vcf) vcf
	, COALESCE(dbfa_ont.new_vcf_size, wfhv_sec.vcf_size) vcf_size
	, NULL tag_user_tags
	, NULL percent_dups
	, NULL percent_q30_bases
	, wfhv_qc.total_seqs
	, wfhv_qc.total_depth median_coverage
	, wfhv_qc.contaminated contamination
	, wfhv_qc.at_least_10x
	, wfhv_qc.at_least_20x
	, wfhv_qc.ploidy_estimation
	, NULL snp
	, wfhv_qc.indel
	, wfhv_qc.ts_tv
	, wfhv_qc.yield
FROM (
	SELECT 
		*
		, ROW_NUMBER() OVER (
		PARTITION BY
			wfhv_analysis.id_repository
		ORDER BY
			date_start DESC
		) rn
	FROM wfhv_analysis
	) wfhv_sec
	LEFT JOIN 
		wfhv_qc 
			ON wfhv_sec.run_name = wfhv_qc.run_name
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
			WHERE sequencer = 'ONT' AND NOT REGEXP_LIKE(id_requestor, '(?i)test')
		) ranked
		WHERE rn = 1
		GROUP BY run_name
	) dbfa_ont 
		ON wfhv_sec.run_name = dbfa_ont.run_name
WHERE
	wfhv_sec.rn = 1
	AND NOT (REGEXP_LIKE(wfhv_sec.id_repository, "(?i)(demo|test|benchmark|dev)") 
		OR REGEXP_LIKE(wfhv_sec.run_name, "(?i)(demo|test|benchmark|dev)"))
);
COMMIT;