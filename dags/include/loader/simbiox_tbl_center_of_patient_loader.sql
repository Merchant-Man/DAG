INSERT INTO simbiox_center_of_patient (
  id, id_patient, origin_code_repository, research_id, project_id, id_biobank, created_at, updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
AS new
ON DUPLICATE KEY UPDATE
  id_patient = new.id_patient,
  origin_code_repository = new.origin_code_repository,
  research_id = new.research_id,
  project_id = new.project_id,
  id_biobank = new.id_biobank,
  updated_at = new.updated_at;
