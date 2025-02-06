INSERT INTO simbiox_biosamples(id,id_patient,code_repository,code_box,code_position,date_received,date_enumerated,id_biobank,origin_code_repository,origin_code_box,biosample_type,biosample_specimen,type_case,sub_cell_specimen,biosample_volume,biosample_status,created_at,updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE id_patient = VALUES(id_patient), code_repository = VALUES(code_repository),
code_box = VALUES(code_box),
code_position = VALUES(code_position),
date_received = VALUES(date_received),
date_enumerated = VALUES(date_enumerated),
id_biobank = VALUES(id_biobank),
origin_code_repository = VALUES(origin_code_repository),
origin_code_box = VALUES(origin_code_box),
biosample_type = VALUES(biosample_type),
biosample_specimen = VALUES(biosample_specimen),
type_case = VALUES(type_case),
sub_cell_specimen = VALUES(sub_cell_specimen),
biosample_volume = VALUES(biosample_volume),
biosample_status = VALUES(biosample_status),
updated_at = '{{ ts }}'