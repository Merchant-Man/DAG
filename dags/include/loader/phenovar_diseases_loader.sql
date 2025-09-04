INSERT INTO phenovar_diseases(
    id,
    hub_id,
    diseases_name,
    created_by,
    updated_by,
    created_at,
    updated_at,
    hub_name
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE 
    hub_id = VALUES(hub_id),
    diseases_name = VALUES(diseases_name),
    created_by = VALUES(created_by),
    updated_by = VALUES(updated_by),
    created_at = VALUES(created_at),
    updated_at = VALUES(updated_at),
    hub_name = VALUES(hub_name);