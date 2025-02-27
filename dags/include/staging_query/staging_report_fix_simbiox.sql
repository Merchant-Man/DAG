INSERT IGNORE staging_report_fix_simbiox(
SELECT
	CURRENT_DATE date_time,
	COUNT(*) ct
FROM
	staging_simbiox_biosamples_patients
WHERE
	registry_sex IS NULL
)