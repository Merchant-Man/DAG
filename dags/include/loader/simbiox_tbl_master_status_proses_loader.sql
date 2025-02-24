INSERT INTO simbiox_master_status_proses (id,status_proses_name,status_proses_color, created_at, updated_at)
VALUES (%s,%s,%s,%s,%s )
ON DUPLICATE KEY UPDATE status_proses_name = VALUES(status_proses_name),status_proses_color = VALUES(status_proses_color), updated_at=VALUES(updated_at)