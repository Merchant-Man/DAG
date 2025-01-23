-- In this loader, we ignore any record (id_patient, PK) that already exists with the assumption that the data is fixed.
INSERT IGNORE INTO simbiox_patients (id_patient, id_mpi, id_subject, sex, date_of_birth, id_biobank, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);