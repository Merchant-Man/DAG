INSERT INTO phenovar_projects(
    id,
    diseases_id,
    name,
    pic,
    institution_id,
    date_start,
    date_end,
    status,
    manage_form_id,
    diseases_name,
    institution_name,
    created_by,
    updated_by,
    created_at,
    updated_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE 
    diseases_id = VALUES(diseases_id),
    name = VALUES(name),
    pic = VALUES(pic),
    institution_id = VALUES(institution_id),
    date_start = VALUES(date_start),
    date_end = VALUES(date_end),
    status = VALUES(status),
    manage_form_id = VALUES(manage_form_id),
    diseases_name = VALUES(diseases_name),
    institution_name = VALUES(institution_name),
    created_by = VALUES(created_by),
    updated_by = VALUES(updated_by),
    updated_at = VALUES(updated_at);
