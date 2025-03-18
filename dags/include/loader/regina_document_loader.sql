INSERT INTO regina_documents(id_subject,composition_id,composition_name,`entry`,created_at,updated_at)
VALUES (%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE composition_id = VALUES(composition_id),
composition_name = VALUES(composition_name),
`entry` = VALUES(`entry`),
updated_at = VALUES(updated_at)