/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as the source of ONQ LIMS secondary and tertiery analysis status.
-- Author   :   Abdullah Faqih
-- Created  :   24-06-2025
-- Changes  :   30-06-2025 Adding pattern to prevent unlucky read access
                28-07-2025 Changing qc column due to upstream changes
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here;


DROP TABLE IF EXISTS gold_lims_report_new;
CREATE TABLE gold_lims_report_new (
    SELECT
        t1.id_repository,
        t1.sequencer,
        
        CASE
            WHEN t1.date_secondary IS NOT NULL THEN TRUE
            ELSE FALSE
        END is_secondary_analysed,
        qc_strict_diagnosis qc_category,
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
ON gold_lims_report_new(id_repository);
CREATE INDEX sequencer_idx
ON gold_lims_report_new(sequencer);
CREATE INDEX is_secondary_analysed_idx
ON gold_lims_report_new(is_secondary_analysed);
CREATE INDEX qc_category_idx
ON gold_lims_report_new(qc_category);
CREATE INDEX is_tertiary_pgx_analysed_idx
ON gold_lims_report_new(is_tertiary_pgx_analysed);


RENAME TABLE
    gold_lims_report       TO gold_lims_report_old,
    gold_lims_report_new   TO gold_lims_report;

-- 4. Drop the old table as cleanup
DROP TABLE IF EXISTS gold_lims_report_old;