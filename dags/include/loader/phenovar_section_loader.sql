INSERT INTO phenovar_section(id,
name,
group_id,
group_name,
created_by,
updated_by,
created_at,
updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE name=VALUES(name),
group_id=VALUES(group_id),
group_name=VALUES(group_name),
created_by=VALUES(created_by),
updated_by=VALUES(updated_by),
created_at=VALUES(created_at),
updated_at=VALUES(updated_at);