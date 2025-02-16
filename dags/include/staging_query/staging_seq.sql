DELETE FROM staging_seq;
INSERT INTO staging_seq
(SELECT
	COALESCE(db_mgi.new_repository, seq_zlims.id_repository) id_repository,
	id_flowcell id_library,
	'MGI' sequencer,
	date_create date_primary,
	NULL sum_of_total_passed_bases,
	NULL sum_of_bam_size,
	COALESCE(db_mgi.new_index, seq_zlims.id_index) id_index
FROM
	zlims_samples seq_zlims
	LEFT JOIN (
		# This zlims is separated since the dynamodb contains redundant rows for MGI data where a code repo could have two rows where one contain index and one not.
		SELECT
			dbt1.id_repository,
			dbt1.new_repository,
			dbt2.id_zlims_index,
			dbt2.new_index
		FROM
			(
				SELECT DISTINCT
					id_repository,
					new_repository
				FROM
					dynamodb_fix_id_repository_latest dbt1
				WHERE
					sequencer = "MGI"
			) dbt1
			LEFT JOIN (
				SELECT DISTINCT
					id_repository,
					id_zlims_index,
					new_index
				FROM
					dynamodb_fix_id_repository_latest
				WHERE
					sequencer = "MGI"
					AND (
						id_zlims_index IS NOT NULL
						AND new_index IS NOT NULL
					)
			) dbt2 ON dbt1.id_repository = dbt2.id_repository
	) db_mgi ON seq_zlims.id_repository = db_mgi.id_repository
UNION ALL
SELECT
	COALESCE(db_ica.new_repository, seq_ica.id_repository) id_repository,
	id_library,
	'Illumina' sequencer,
	time_modified date_primary,
	NULL sum_of_total_passed_bases,
	NULL sum_of_bam_size,
	NULL id_index
FROM
	ica_samples seq_ica
	LEFT JOIN (
		SELECT DISTINCT
			id_repository,
			new_repository
		FROM
			dynamodb_fix_id_repository_latest
		WHERE
			sequencer = "Illumina"
	) db_ica ON seq_ica.id_repository = db_ica.id_repository
UNION ALL
SELECT
	COALESCE(db_wfhv.new_repository, seq_wfhv.id_repository) id_repository,
	id_batch id_library,
	"ONT" sequencer,
	date_upload date_primary,
	sum_of_total_passed_bases,
	sum_of_bam_size,
	NULL id_index
FROM
	wfhv_samples seq_wfhv
	LEFT JOIN (
		SELECT DISTINCT
			id_repository,
			new_repository
		FROM
			dynamodb_fix_id_repository_latest
		WHERE
			sequencer = "ONT"
	) db_wfhv ON seq_wfhv.id_repository = db_wfhv.id_repository)