INSERT INTO zlims_samples(id_repository,id_flowcell,id_pool,id_dnb,id_index,date_create,created_at,updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE id_repository = VALUES(id_repository),
id_flowcell = VALUES(id_flowcell),
id_pool = VALUES(id_pool),
id_dnb = VALUES(id_dnb),
id_index = VALUES(id_index),
date_create = VALUES(date_create),
updated_at = '{{ ts }}'