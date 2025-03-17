INSERT INTO phenovar_categories(id,`name`,group_id,group_name,section_id,section_name,`status`,created_by,updated_by,created_at,updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE `name` = VALUES(`name`),
group_id = VALUES(group_id),
group_name = VALUES(group_name),
section_id = VALUES(section_id),
section_name = VALUES(section_name),
`status` = VALUES(`status`),
updated_by = VALUES(updated_by),
updated_at = VALUES(updated_at)