INSERT INTO phenovar_data_sharing(
    id,
    ethical_clearance_id,
    ethical_clearance_title,
    id_subject,
    institution_owner_id,
    institution_owner_name,
    institution_share_ids,
    gender,
    dob,
    province,
    district,
    hub_name,
    use_nik_ibu,
    access,
    created_at,
    updated_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE 
ethical_clearance_id = VALUES(ethical_clearance_id),
ethical_clearance_title = VALUES(ethical_clearance_title),
id_subject = VALUES(id_subject),
institution_owner_id = VALUES(institution_owner_id),
institution_owner_name = VALUES(institution_owner_name),
institution_share_ids = VALUES(institution_share_ids),
gender = VALUES(gender),
dob = VALUES(dob),
province = VALUES(province),
district = VALUES(district),
hub_name = VALUES(hub_name),
use_nik_ibu = VALUES(use_nik_ibu),
access = VALUES(access),
updated_at = VALUES(updated_at)
;
