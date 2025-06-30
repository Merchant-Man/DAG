/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as the source of QC metrics.
-- Author   :   Abdullah Faqih
-- Created  :   16-02-2025
-- Changes	: 
				02-03-2025: Adding filter for testing id repositories
				30-06-2025 Adding transcation lock to the query
---------------------------------------------------------------------------------------------------------------------------------
*/
START TRANSACTION;
DELETE FROM staging_ont_sec;
INSERT INTO staging_ont_sec
-- TODO - In the future it might be that we would have an id repo to be rerun with different run_name. Latest run_time should be chosen.
	(SELECT
	date_start,
	wfhv_analysis.id_repository,
	NULL id_batch,
	wfhv_analysis.pipeline_name,
	wfhv_analysis.run_name,
	wfhv_analysis.cram,
	wfhv_analysis.cram_size,
	wfhv_analysis.vcf,
	wfhv_analysis.vcf_size,
	NULL tag_user_tags,
	NULL percent_dups,
	NULL percent_q30_bases,
	wfhv_qc.total_seqs,
	wfhv_qc.total_depth median_coverage,
	wfhv_qc.contaminated contamination,
	wfhv_qc.at_least_10x,
	wfhv_qc.at_least_20x,
	wfhv_qc.ploidy_estimation,
	NULL snp,
	wfhv_qc.indel,
	wfhv_qc.ts_tv,
	wfhv_qc.yield
FROM
	wfhv_analysis
	LEFT JOIN wfhv_qc ON wfhv_analysis.id_repository = wfhv_qc.id_repository
	AND wfhv_analysis.run_name = wfhv_qc.run_name
WHERE
	NOT (REGEXP_LIKE(wfhv_analysis.id_repository, "(?i)(demo|test|benchmark|dev)") OR REGEXP_LIKE(wfhv_analysis.run_name, "(?i)(demo|test|benchmark|dev)")));
COMMIT;