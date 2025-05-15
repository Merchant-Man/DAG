/*
---------------------------------------------------------------------------------------------------------------------------------
-- Purpose  :   This query is created to provide data for transfer processes.
-- Author   :   Abdullah Faqih
-- Created  :   14-05-2025
-- Changes	:   
---------------------------------------------------------------------------------------------------------------------------------
*/
DELETE FROM staging_simbiox_transfer;
INSERT INTO
	staging_simbiox_transfer (
SELECT 
	biosample_id,
	id,
	nomor_formulir,
	tanggal_formulir,
	biobank_asal,
	biobank_tujuan, 
	tanggal_pengiriman,
	waktu_pengiriman,
	suhu_pengiriman,
	keterangan_pengiriman,
	status_pengiriman,
	petugas_pengirim,
	tanggal_penerimaan,
	waktu_penerimaan,
	suhu_penerimaan,
	keterangan_penerimaan,
	status_penerimaan,
	petugas_penerima,
	status_transfer_name
FROM (
SELECT 	
	t1.biosample_id,
	t2.id,
	t2.nomor_formulir,
	t2.tanggal_formulir,
	t6.biobank_nama biobank_asal,
	t7.biobank_nama biobank_tujuan, 
	t2.tanggal_pengiriman,
	t2.waktu_pengiriman,
	t2.suhu_pengiriman,
	t2.keterangan keterangan_pengiriman,
	t4.status_proses_name status_pengiriman,
	t2.petugas_pengirim,
	t2.tanggal_penerimaan,
	t2.waktu_penerimaan,
	t2.suhu_penerimaan,
	t2.keterangan_receive keterangan_penerimaan,
	t5.status_proses_name status_penerimaan,
	t2.petugas_penerima,
	t8.status_transfer_name,
	ROW_NUMBER() OVER(PARTITION BY t1.biosample_id ORDER BY tanggal_penerimaan DESC) rn
FROM
	simbiox_transfer t1
	LEFT JOIN simbiox_master_status_proses t4 ON t1.proses_status = t4.id
	LEFT JOIN simbiox_master_status_proses t5 ON t1.proses_receive_status = t5.id
	LEFT JOIN master_biobank t6 ON t1.id_biobank = t6.id_biobank
	LEFT JOIN master_biobank t7 ON t1.id_biobank_tujuan = t7.id_biobank
	LEFT JOIN simbiox_transfer_formulir t2 ON t1.transfer_formulir_id = t2.id
	LEFT JOIN simbiox_master_status_transfer t8 ON t2.transfer_status = t8.id
) t
WHERE rn = 1


	)