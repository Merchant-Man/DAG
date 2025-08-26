/*
 ---------------------------------------------------------------------------------------------------------------------------------
 -- Purpose  :   This query is intended to be used as the staging results of illumina primary analysis data.
 -- Author   :   Renata Triwijaya
 -- Created  :   27-08-2025
 -- Changes	 :	 
 ---------------------------------------------------------------------------------------------------------------------------------
 */
-- Your SQL code goes here
START TRANSACTION;
DELETE FROM staging_illumina_primary;
INSERT INTO staging_illumina_primary
(
    SELECT 
        samples.id_repository
        runs.id_library
        , runs.session_id
        , runs.session_name
        , runs.date_created
        , runs.date_modified
        , runs.execution_status
        , runs.workflow_reference
        , runs.run_id
        , runs.run_name
        , runs.percent_gt_q30
        , runs.flowcell_barcode
        , runs.status
        , runs.run_date_created
        , runs.total_flowcell_yield_Gbps
        , demux.total_reads
        , demux.percent_reads
        , qs.yield
        , qs.yield_q30
    FROM illumina_biosamples samples
    LEFT JOIN
        illumina_runs runs
        ON samples.session_id=runs.session_id
    LEFT JOIN (
        SELECT
            id_repository
            , id_library
            , SUM(yield) yield
            , SUM(yield_q30) yield_q30
        FROM illumina_qs
        GROUP BY
            id_repository
            , id_library
        ) qs
            ON samples.id_repository = qs.id_repository
                AND samples.id_library = qs.id_library
    LEFT JOIN (
        SELECT
            id_repository
            , id_library
            , SUM(reads) total_reads
            , SUM(percent_reads) percent_reads
        FROM illumina_demux
        GROUP BY
            id_repository
            , id_library 
    ) demux
        ON samples.id_repository = demux.id_repository
            AND samples.id_library = demux.id_library
);
COMMIT;