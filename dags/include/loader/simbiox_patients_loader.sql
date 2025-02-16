-- In this loader, we ignore any record (id_patient, PK) that already exists with the assumption that the data is fixed.
INSERT INTO simbiox_patients (id_patient, id_mpi, id_subject, sex, date_of_birth, id_biobank, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE id_mpi = VALUES(id_mpi), id_subject = VALUES(id_subject),  sex = VALUES(sex), date_of_birth = VALUES(date_of_birth), id_biobank = VALUES(id_biobank), updated_at = '{{ ts }}'