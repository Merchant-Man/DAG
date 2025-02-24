INSERT INTO simbiox_master_status_transfer (id,status_transfer_name,status_transfer_color,created_at, updated_at)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE status_transfer_name = VALUES(status_transfer_name),
status_transfer_color = VALUES(status_transfer_color), updated_at = VALUES(updated_at) 