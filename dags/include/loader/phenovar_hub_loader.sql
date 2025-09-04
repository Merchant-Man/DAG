INSERT INTO phenovar_hubs(
    id,
    name,
    status,
    notes,
    created_by,
    updated_by,
    created_at,
    updated_at,
    deleted_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE 
    name = VALUES(`name`),
    status = VALUES(`status`),
    notes = VALUES(notes),
    created_by = VALUES(created_by),
    updated_by = VALUES(updated_by),
    created_at = VALUES(created_at),
    updated_at = VALUES(updated_at),
    deleted_at = VALUES(deleted_at);