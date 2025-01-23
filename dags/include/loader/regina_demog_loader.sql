INSERT INTO regina_demography (id_subject, age, sex, created_at, updated_at, creation_date)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE age = VALUES(age), sex = VALUES(sex)