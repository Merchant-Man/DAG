/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as the source of QC metrics.
-- Author   :   Renata
-- Created  :   10-02-2025
-- Changes:
                13-02-2025 - Added a new column to the output - Abdullah Faqih
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here 
CREATE TABLE gold_qc_metrics
(WITH cte AS (
    SELECT DISTINCT *,
        CASE
            WHEN COUNT(CASE WHEN sex_ploidy_category = 'Mismatch' THEN 1 END) 
                OVER (PARTITION BY id_library) > 0 THEN 'Fail'
            WHEN COUNT(CASE WHEN sex_ploidy_category = 'No Data' THEN 1 END) 
                OVER (PARTITION BY id_library) > 0 THEN 'Incomplete Data'
            ELSE 'Pass'
        END AS batch_sex_category
    FROM (
        SELECT 
            seq.sequencer, 
            seq.date_primary, sp.id_subject, sb.id_patient, mb.biobank_nama AS origin_biobank, 
--             sb.date_received, sb.biosample_type, sb.biosample_specimen, 
			r.sex, r.id_subject as id_subject_r,

            -- Replace id_repository with new_repository if it's not empty
            CASE 
                WHEN db.new_repository IS NOT NULL AND db.new_repository != '' THEN db.new_repository 
                ELSE seq.id_repository 
            END AS id_repository,

            -- Handle id_library updates
            CASE 
                WHEN seq.sequencer = 'Illumina' THEN COALESCE(sec_ica.id_batch, seq.id_library)
                WHEN db.new_library IS NOT NULL AND db.new_library != '' THEN db.new_library 
                ELSE seq.id_library
            END AS id_library,

            -- Handle id_index_zlims updates
            CASE 
                WHEN db.new_index IS NOT NULL AND db.new_index != '' THEN db.new_index 
                ELSE seq.id_index
            END AS id_index_zlims,

            COALESCE(sec_mgi.run_name, sec_ica.run_name) AS run_name,
            COALESCE(sec_mgi.pipeline_name, sec_ica.pipeline_name) AS pipeline_name,
            COALESCE(sec_mgi.date_start, sec_ica.date_start) AS date_secondary,
            COALESCE(sec_mgi.pipeline_type, sec_ica.pipeline_type) AS pipeline_type,
            COALESCE(sec_mgi.run_status, sec_ica.run_status) AS run_status,
--             COALESCE(sec_mgi.cram, sec_ica.cram) AS cram, 
--             COALESCE(sec_mgi.cram_size, sec_ica.cram_size) AS cram_size,
            COALESCE(sec_mgi.vcf, sec_ica.vcf) AS vcf, 
            COALESCE(sec_mgi.vcf_size, sec_ica.vcf_size) AS vcf_size,
--             COALESCE(sec_mgi.id_batch, sec_ica.id_batch) AS id_batch,
            
--             sec_mgi.id_index_analysis,
            
            -- QC   
            COALESCE(mgi_qc.at_least_10x, illumina_qc.at_least_10x) AS at_least_10x,
            COALESCE(mgi_qc.at_least_20x, illumina_qc.at_least_20x) AS at_least_20x,
--             COALESCE(mgi_qc.total_seqs, illumina_qc.total_seqs) AS total_seqs,
            COALESCE(mgi_qc.ploidy_estimation, illumina_qc.ploidy_estimation) AS ploidy_estimation,
            COALESCE(mgi_qc.median_coverage, illumina_qc.median_coverage) AS coverage,
--             COALESCE(mgi_qc.snp, illumina_qc.snp) AS snp,
--             COALESCE(mgi_qc.indel, illumina_qc.indel) AS indel,
--             COALESCE(mgi_qc.ts_tv, illumina_qc.ts_tv) AS ts_tv,
            COALESCE(illumina_qc.contam) AS contamination, 
--             illumina_qc.percent_dups, illumina_qc.percent_q30_bases, 
--             illumina_qs.yield, illumina_qs.yield_q30,
            
            CASE
                WHEN UPPER(COALESCE(mgi_qc.ploidy_estimation, illumina_qc.ploidy_estimation)) = 'XX' AND UPPER(r.sex) = 'FEMALE' THEN 'Match'
                WHEN UPPER(COALESCE(mgi_qc.ploidy_estimation, illumina_qc.ploidy_estimation)) = 'XY' AND UPPER(r.sex) = 'MALE' THEN 'Match'
                WHEN r.sex IS NULL THEN 'No Data'
                WHEN COALESCE(mgi_qc.ploidy_estimation, illumina_qc.ploidy_estimation) IS NULL THEN 'No Data'
                ELSE 'Mismatch'
            END AS sex_ploidy_category

        FROM ( 
            SELECT NULL AS id, id_repository, id_flowcell AS id_library, id_index AS id_index, date_create AS date_primary, 'MGI' AS sequencer FROM zlims_samples  
            UNION ALL
            SELECT NULL AS id, id_repository, id_library, NULL AS id_index, time_modified AS date_primary, 'Illumina' AS sequencer FROM ica_samples
        ) seq
        LEFT JOIN simbiox_biosamples sb ON seq.id_repository = sb.code_repository
        LEFT JOIN master_biobank mb ON sb.id_biobank = mb.id_biobank
        LEFT JOIN simbiox_patients sp ON sb.id_patient = sp.id_patient
        LEFT JOIN (
            SELECT id_subject, sex FROM regina_demography 
            UNION ALL
            SELECT id_subject, sex FROM phenovar_participants 
        ) AS r ON sp.id_subject = r.id_subject
        LEFT JOIN mgi_analysis sec_mgi ON seq.id_repository = sec_mgi.id_repository AND seq.sequencer= 'MGI'
        LEFT JOIN ica_analysis sec_ica ON seq.id_repository = sec_ica.id_repository AND seq.sequencer= 'Illumina'
        LEFT JOIN (
			SELECT id_repository, depth, at_least_10x, at_least_20x, total_seqs, median_coverage, non_primary, percent_mapped, percent_proper_pairs, reads_mapped, vars, snp, indel, ts_tv,
				CASE
					WHEN ploidy_estimation = 'female' THEN 'XX'
					WHEN ploidy_estimation = 'male' THEN 'XY'
					ELSE UPPER(ploidy_estimation)
				END AS ploidy_estimation
			FROM mgi_qc
		) mgi_qc ON sb.code_repository = mgi_qc.id_repository AND seq.sequencer= 'MGI'
        LEFT JOIN illumina_qc ON sb.code_repository = illumina_qc.id_repository AND seq.sequencer= 'Illumina'
        LEFT JOIN (
            SELECT id_repository, SUM(yield) AS yield, SUM(yield_q30) AS yield_q30
            FROM illumina_qs
            GROUP BY id_repository
        ) illumina_qs ON sb.code_repository = illumina_qs.id_repository AND seq.sequencer='Illumina'
        LEFT JOIN dynamodb_fix_id_repository_latest AS db ON seq.id_repository = db.id_repository
		
        WHERE mb.biobank_nama NOT IN ('Biobank Sentral (BB Binomika)','Biobank Pusat')
	        AND seq.sequencer IS NOT NULL
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

SELECT *,
    CASE 
        WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NOT NULL THEN 'No Data'
        WHEN sex IS NULL AND (coverage IS NOT NULL AND at_least_10x IS NOT NULL) AND batch_sex_category <> 'Fail' THEN 'No Data'
        WHEN sex IS NULL AND coverage IS NULL AND at_least_10x IS NULL THEN 'No Data'
        WHEN coverage >= 30 AND at_least_10x >= 90 THEN 
            CASE 
                WHEN batch_sex_category = 'Pass' THEN 'Pass'
                WHEN batch_sex_category = 'Incomplete Data' THEN 'No Data'
                ELSE 'Fail'
            END
        ELSE 'Fail'
    END AS qc_category,
    CASE 
    -- Check for "No Data" first (most restrictive)
    WHEN sex IS NULL AND coverage IS NULL AND at_least_10x IS NULL THEN 'No Data'
    
    -- Check for "No Coverage Data" (sex is present, but coverage data is missing)
    WHEN (coverage IS NULL OR at_least_10x IS NULL) AND sex IS NOT NULL THEN 'No Coverage Data'
    
    -- Check for "No Sex Data" only when batch_sex_category is NOT Fail
    WHEN sex IS NULL AND (coverage IS NOT NULL AND at_least_10x IS NOT NULL) AND batch_sex_category <> 'Fail' THEN 'No Sex Data'

    -- Specific QC Pass or Fail logic
    WHEN coverage >= 30 AND at_least_10x >= 90 THEN 
        CASE 
            WHEN batch_sex_category = 'Pass' THEN 'Pass'
            WHEN sex IS NOT NULL AND batch_sex_category = 'Incomplete Data' THEN 'Incomplete Batch Sex Data'
            ELSE 'Fail Sex Check'
        END
    
    -- Fail QC logic
    WHEN coverage < 30 AND at_least_10x < 90 AND batch_sex_category = 'Fail' THEN 'Fail QC'

    -- Catch-all for other Coverage QC failures
    ELSE 'Fail Coverage QC'
END AS qc_category2

 FROM cte)