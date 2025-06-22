/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as the source of PGx report data.
-- Author   :   Abdullah Faqih
-- Created  :   24-02-2025
-- Changes  :   13-06-2025: Adding indexes for performance improvement
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here DROP TABLE IF EXISTS  gold_pgx_report;
DROP TABLE IF EXISTS gold_pgx_report;

CREATE TABLE gold_pgx_report (
SELECT
    *
    ,CASE
        WHEN pgx_input_creation_date IS NOT NULL AND (eng_report_creation_date IS NULL OR ind_report_creation_date IS NULL ) THEN "Failed Run"
        WHEN pgx_input_creation_date IS NOT NULL AND (eng_report_creation_date IS NOT NULL OR ind_report_creation_date IS NOT NULL ) THEN "Successful Run"
        WHEN pgx_input_creation_date IS NULL AND (eng_report_creation_date IS  NULL AND ind_report_creation_date IS NULL ) THEN "Uncompleted Run"
    END AS pgx_status
FROM (
	SELECT
		gold_qc.id_repository,
		gold_qc.id_patient,
		gold_qc.id_subject,
		prs.file_name,
		gold_qc.origin_biobank,
		gold_qc.sex,
		gold_qc.sequencer,
		date_primary,
		date_secondary,
		gold_qc.run_name,
		prs.input_creation_date pgx_input_creation_date,
		CASE
			WHEN prs.report_path_eng = "nan" THEN NULL
			ELSE prs.report_path_eng
		END report_path_eng,
		CASE
			WHEN prs.eng_report_creation_date = "0000-00-00 00:00:00" THEN NULL
			ELSE prs.eng_report_creation_date
		END eng_report_creation_date,
		CASE
			WHEN prs.report_path_ind = "nan" THEN NULL
			ELSE prs.report_path_ind
		END report_path_ind,
		CASE
			WHEN prs.ind_report_creation_date = "0000-00-00 00:00:00" THEN NULL
			ELSE prs.ind_report_creation_date
		END ind_report_creation_date,
        CURRENT_TIMESTAMP udpated_at
	FROM
		gold_qc
		LEFT JOIN (SELECT * FROM (SELECT
	*,
	ROW_NUMBER() OVER (
		PARTITION BY
			id_repository
		ORDER BY
			input_creation_date DESC, report_path_ind DESC
	) rn
FROM
	superset_dev.staging_pgx_report_status) t WHERE rn = 1) prs ON gold_qc.id_repository = prs.id_repository
	WHERE
		gold_qc.qc_category2 = "Pass"
) t
);

CREATE INDEX id_repository_idx
ON gold_pgx_report(id_repository);
CREATE INDEX origin_biobank_idx
ON gold_pgx_report(origin_biobank);
CREATE INDEX sequencer_idx
ON gold_pgx_report(sequencer);
CREATE INDEX file_name_idx ON gold_pgx_report (file_name);