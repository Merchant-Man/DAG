INSERT INTO simbiox_data_project (
  id_project, id_pi, institution, ket_institution, datamanager,
  teknisi, tim, titleoftheproject, startproject, endproject,
  descriptionofproject, projectfunding, create_time, create_by,
  id_biobank, id_urut, project_kode, created_at, updated_at
)
VALUES (
  %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s,
  %s, %s, %s, %s,
  %s, %s, %s, %s, %s
)
AS new
ON DUPLICATE KEY UPDATE
  id_pi = new.id_pi,
  institution = new.institution,
  ket_institution = new.ket_institution,
  datamanager = new.datamanager,
  teknisi = new.teknisi,
  tim = new.tim,
  titleoftheproject = new.titleoftheproject,
  startproject = new.startproject,
  endproject = new.endproject,
  descriptionofproject = new.descriptionofproject,
  projectfunding = new.projectfunding,
  create_time = new.create_time,
  create_by = new.create_by,
  id_biobank = new.id_biobank,
  id_urut = new.id_urut,
  project_kode = new.project_kode,
  created_at = new.created_at,
  updated_at = new.updated_at;
