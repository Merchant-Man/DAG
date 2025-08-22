-- Load BCLConvert session data
INSERT INTO bclconvert_appsessions (
    row_type
    , session_id
    , session_name
    , date_created
    , date_modified
    , execution_status
    , ica_link
    , ica_project_id
    , workflow_reference
    , run_id
    , run_name
    , percent_gt_q30
    , flowcell_barcode
    , reagent_barcode
    , status
    , experiment_name
    , run_date_created
    , id_repository
    , biosample_id
    , computed_yield_bps
    , generated_sample_id
    , created_at
    , updated_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

ON DUPLICATE KEY UPDATE
row_type = VALUES(row_type),
session_id = VALUES(session_id),
session_name = VALUES(session_name),
date_created = VALUES(date_created),
date_modified = VALUES(date_modified),
execution_status = VALUES(execution_status),
ica_link = VALUES(ica_link),
ica_project_id = VALUES(ica_project_id),
workflow_reference = VALUES(workflow_reference),
run_id = VALUES(run_id),
run_name = VALUES(run_name),
percent_gt_q30 = VALUES(percent_gt_q30),
flowcell_barcode = VALUES(flowcell_barcode),
reagent_barcode = VALUES(reagent_barcode),
status = VALUES(status),
experiment_name = VALUES(experiment_name),
run_date_created = VALUES(run_date_created),
bio_sample_name = VALUES(bio_sample_name),
bio_sample_id = VALUES(id_repository),
computed_yield_bps = VALUES(computed_yield_bps),
generated_sample_id = VALUES(generated_sample_id),
updated_at = '{{ ts }}';

