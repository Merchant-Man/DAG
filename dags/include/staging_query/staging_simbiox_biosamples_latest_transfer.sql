/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is created to provide a materialised table on simbiox biosamples with their latest state for staging purpose.
-- Author   :   Abdullah Faqih
-- Created  :   04-07-2025
-- Changes	:  
---------------------------------------------------------------------------------------------------------------------------------
*/

START TRANSACTION;
DROP TABLE IF EXISTS staging_simbiox_biosamples_latest_transfer;
CREATE TABLE staging_simbiox_biosamples_latest_transfer
(
WITH
	latest_trf AS (
		SELECT
			*
		FROM
			(
				SELECT
					*,
					ROW_NUMBER() OVER (
						PARTITION BY
							biosample_id
						ORDER BY
							tanggal_formulir DESC
					) rn
				FROM
					gold_simbiox_transfer
			) t
		WHERE
			t.rn = 1
	)
SELECT
	DISTINCT
	sp.id_subject,
	sb.origin_code_repository,
	mb.biobank_nama,
	sb.code_repository,
	ms.specimen_type_name,
	gst.nomor_formulir,
	gst.tanggal_formulir,
	gst.status_transfer_name,
	gst.biobank_asal,
	gst.biobank_tujuan,
	gst.tanggal_penerimaan,
	gst.status_penerimaan,
	gst.tujuan_is_biobank,
	gst.is_to_central_seq
FROM
	simbiox_biosamples sb
	LEFT JOIN master_specimen ms ON sb.biosample_specimen = ms.id
	LEFT JOIN simbiox_patients sp ON sb.id_patient = sp.id_patient
	LEFT JOIN master_biobank mb ON sp.id_biobank = mb.id_biobank
	LEFT JOIN latest_trf gst ON sb.id = gst.biosample_id
);
CREATE INDEX status_penerimaan_idx
ON staging_simbiox_biosamples_latest_transfer(status_penerimaan);
CREATE INDEX specimen_type_name_idx
ON staging_simbiox_biosamples_latest_transfer(specimen_type_name);
CREATE INDEX origin_code_repository_idx
ON staging_simbiox_biosamples_latest_transfer(origin_code_repository);
CREATE INDEX is_to_central_seq_idx
ON staging_simbiox_biosamples_latest_transfer(is_to_central_seq);
CREATE INDEX id_subject_idx
ON staging_simbiox_biosamples_latest_transfer(id_subject);
CREATE INDEX code_repository_idx
ON staging_simbiox_biosamples_latest_transfer(code_repository);
CREATE INDEX biobank_tujuan_idx
ON staging_simbiox_biosamples_latest_transfer(biobank_tujuan);
CREATE INDEX biobank_nama_idx
ON staging_simbiox_biosamples_latest_transfer(biobank_nama);
COMMIT;