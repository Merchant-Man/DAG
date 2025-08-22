INSERT INTO bclconvert_appsessions (
  id_repository,
  row_type, session_id, session_name, date_created, date_modified,
  execution_status, ica_link, ica_project_id, workflow_reference,
  run_id, run_name, percent_gt_q30, flowcell_barcode, reagent_barcode,
  `status`, experiment_name,
  run_date_created,
  biosample_id,
  computed_yield_bps, generated_sample_id,
  `yield`, total_flowcell_yield,
  reads_total, reads_perfect_index, reads_one_mismatch_index, reads_two_mismatch_index, reads_percent,
  updated_at, created_at
)
VALUES (
  %(id_repository)s,
  %(row_type)s, %(session_id)s, %(session_name)s, %(date_created)s, %(date_modified)s,
  %(execution_status)s, %(ica_link)s, %(ica_project_id)s, %(workflow_reference)s,
  %(run_id)s, %(run_name)s, %(percent_gt_q30)s, %(flowcell_barcode)s, %(reagent_barcode)s,
  %(status)s, %(experiment_name)s,
  -- parse ISO like 2025-08-05T08:38:40.0000000Z to DATETIME(6)
  STR_TO_DATE(SUBSTRING_INDEX(REPLACE(REPLACE(%(run_date_created)s,'T',' '),'Z',''),'.',2),
              '%Y-%m-%d %H:%i:%s.%f'),
  %(biosample_id)s,
  %(computed_yield_bps)s, %(generated_sample_id)s,
  %(yield)s, %(total_flowcell_yield)s,
  %(reads_total)s, %(reads_perfect_index)s, %(reads_one_mismatch_index)s, %(reads_two_mismatch_index)s, %(reads_percent)s,
  NOW(6), NOW(6)
)
ON DUPLICATE KEY UPDATE
  -- update only if incoming run_date_created is newer (or stored is NULL)
  row_type             = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(row_type),             row_type),
  session_id           = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(session_id),           session_id),
  session_name         = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(session_name),         session_name),
  date_modified        = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(date_modified),        date_modified),
  execution_status     = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(execution_status),     execution_status),
  ica_link             = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(ica_link),             ica_link),
  ica_project_id       = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(ica_project_id),       ica_project_id),
  workflow_reference   = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(workflow_reference),   workflow_reference),
  run_id               = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(run_id),               run_id),
  run_name             = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(run_name),             run_name),
  percent_gt_q30       = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(percent_gt_q30),       percent_gt_q30),
  flowcell_barcode     = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(flowcell_barcode),     flowcell_barcode),
  reagent_barcode      = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(reagent_barcode),      reagent_barcode),
  `status`             = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(`status`),             `status`),
  experiment_name      = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(experiment_name),      experiment_name),
  run_date_created     = GREATEST(run_date_created, VALUES(run_date_created)),
  biosample_id         = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(biosample_id),         biosample_id),
  computed_yield_bps   = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(computed_yield_bps),   computed_yield_bps),
  generated_sample_id  = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(generated_sample_id),  generated_sample_id),
  `yield`              = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(`yield`),              `yield`),
  total_flowcell_yield = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(total_flowcell_yield), total_flowcell_yield),
  reads_total                  = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(reads_total),                  reads_total),
  reads_perfect_index          = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(reads_perfect_index),          reads_perfect_index),
  reads_one_mismatch_index     = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(reads_one_mismatch_index),     reads_one_mismatch_index),
  reads_two_mismatch_index     = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(reads_two_mismatch_index),     reads_two_mismatch_index),
  reads_percent                = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(reads_percent),                reads_percent),
  -- stamp only when we accept the newer row
  updated_at           = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, NOW(6),                       updated_at);



