/*
 ---------------------------------------------------------------------------------------------------------------------------------
 -- Purpose  :   This query is created to switch any SKI code repo into existing, real id repositories..
 -- Author   :   Abdullah Faqih
 -- Created  :   13-06-2025
 -- Changes	 :	 30-06-2025 Adding transcation lock to the query
 ---------------------------------------------------------------------------------------------------------------------------------
 */
-- Your SQL code goes here
START TRANSACTION;
DELETE FROM staging_fix_ski_id_repo;
INSERT INTO staging_fix_ski_id_repo(
	SELECT
		code_repository,
		origin_code_repository,
		CONCAT(REGEXP_SUBSTR(origin_code_repository, "SKI"), "_", REGEXP_SUBSTR(origin_code_repository, "\\d+")) new_origin_code_repository
	FROM
		simbiox_biosamples
	WHERE
		origin_code_repository LIKE "%SKI%"
);
COMMIT;