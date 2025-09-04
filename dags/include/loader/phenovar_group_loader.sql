INSERT INTO phenovar_group(
    id,
    `name`,
    created_by,
    updated_by,
    created_at,
    updated_at
)
VALUES (%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE 
    `name` = VALUES(`name`),
    created_by = VALUES(created_by),
    updated_by = VALUES(updated_by),
    created_at = VALUES(created_at),
    updated_at = VALUES(updated_at);
