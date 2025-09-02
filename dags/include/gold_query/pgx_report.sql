/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is intended to be used as the source of PGx report data.
-- Author   :   Abdullah Faqih
-- Created  :   24-02-2025
-- Changes  :   13-06-2025 Adding indexes for performance improvement
				30-06-2025 Adding pattern to prevent unlucky read access
				30-08-2025 Adding patient category
---------------------------------------------------------------------------------------------------------------------------------
*/

-- Your SQL code goes here DROP TABLE IF EXISTS  gold_pgx_report;
DROP TABLE IF EXISTS gold_pgx_report_new;
CREATE TABLE gold_pgx_report_new(
SELECT
    t1.*
    ,CASE
        WHEN pgx_input_creation_date IS NOT NULL AND (eng_report_creation_date IS NULL AND ind_report_creation_date IS NULL ) THEN "Failed Run"
        WHEN pgx_input_creation_date IS NOT NULL AND (eng_report_creation_date IS NOT NULL OR ind_report_creation_date IS NOT NULL ) THEN "Successful Run"
        WHEN pgx_input_creation_date IS NULL AND (eng_report_creation_date IS  NULL AND ind_report_creation_date IS NULL ) THEN "Uncompleted Run"
    END AS pgx_status
    ,t2.participant_type
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
		gold_qc.qc_strict_status = "Pass"
) t1
LEFT JOIN
	staging_demography t2 ON t1.id_subject = t2.id_subject
);

CREATE INDEX id_repository_idx
ON gold_pgx_report_new(id_repository);
CREATE INDEX origin_biobank_idx
ON gold_pgx_report_new(origin_biobank);
CREATE INDEX sequencer_idx
ON gold_pgx_report_new(sequencer);
CREATE INDEX file_name_idx 
ON gold_pgx_report_new (file_name);


RENAME TABLE
    gold_pgx_report       TO gold_pgx_report_old,
    gold_pgx_report_new   TO gold_pgx_report;

-- 4. Drop the old table as cleanup
DROP TABLE IF EXISTS gold_pgx_report_old;