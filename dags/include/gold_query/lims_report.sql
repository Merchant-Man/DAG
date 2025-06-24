/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as the source of ONQ LIMS secondary and tertiery analysis status.
-- Author   :   Abdullah Faqih
-- Created  :   24-06-2025
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here;
DROP TABLE IF EXISTS gold_lims_report;

CREATE TABLE gold_lims_report (
    SELECT
        t1.id_repository,
        t1.sequencer,
        
        CASE
            WHEN t1.date_secondary IS NOT NULL THEN TRUE
            ELSE FALSE
        END is_secondary_analysed,
        CASE
            WHEN t1.date_secondary IS NULL THEN NULL
            WHEN t1.qc_category != 'Pass' THEN 'Failed'
            ELSE 'Pass'
        END qc_category,
        CASE
            WHEN t2.eng_report_creation_date IS NOT NULL OR t2.ind_report_creation_date IS NOT NULL THEN TRUE ELSE FALSE END is_tertiary_pgx_analysed
    FROM
        gold_qc t1
    LEFT JOIN
        gold_pgx_report t2
    ON
        t1.id_repository = t2.id_repository
);

CREATE INDEX id_repository_idx
ON gold_lims_report(id_repository);
CREATE INDEX sequencer_idx
ON gold_lims_report(sequencer);
CREATE INDEX is_secondary_analysed_idx
ON gold_lims_report(is_secondary_analysed);
CREATE INDEX qc_category_idx
ON gold_lims_report(qc_category);
CREATE INDEX is_tertiary_pgx_analysed_idx
ON gold_lims_report(is_tertiary_pgx_analysed);