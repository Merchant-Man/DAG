INSERT INTO regina_demography (id_subject, age, sex, created_at, updated_at, creation_date, ehr_id)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE age = VALUES(age), sex = VALUES(sex), updated_at = '{{ ts }}', ehr_id = VALUES(ehr_id);