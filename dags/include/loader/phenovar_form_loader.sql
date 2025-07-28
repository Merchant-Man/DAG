INSERT INTO phenovar_form(id,
diseases_id,
name,
version,
status,
created_by,
updated_by,
created_at,
updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE diseases_id=VALUES(diseases_id),
name=VALUES(name),
version=VALUES(version),
status=VALUES(status),
created_by=VALUES(created_by),
updated_by=VALUES(updated_by),
created_at=VALUES(created_at),
updated_at=VALUES(updated_at)