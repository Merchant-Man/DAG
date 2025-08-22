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
    , id_repository      -- unique key (from BioSampleName)
    , biosample_id
    , computed_yield_bps
    , generated_sample_id
    , created_at
    , updated_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    row_type = VALUES(row_type)
    , session_id = VALUES(session_id)
    , session_name = VALUES(session_name)
    , date_created = VALUES(date_created)
    , date_modified = VALUES(date_modified)
    , execution_status = VALUES(execution_status)
    , ica_link = VALUES(ica_link)
    , ica_project_id = VALUES(ica_project_id)
    , workflow_reference = VALUES(workflow_reference)
    , run_id = VALUES(run_id)
    , run_name = VALUES(run_name)
    , percent_gt_q30 = VALUES(percent_gt_q30)
    , flowcell_barcode = VALUES(flowcell_barcode)
    , reagent_barcode = VALUES(reagent_barcode)
    , status = VALUES(status)
    , experiment_name = VALUES(experiment_name)
    , run_date_created = VALUES(run_date_created)
    , id_repository = VALUES(id_repository)
    , biosample_id = VALUES(biosample_id)
    , computed_yield_bps = VALUES(computed_yield_bps)
    , generated_sample_id = VALUES(generated_sample_id)
    , updated_at = {{ ts }}
    -- update only if incoming run_date_created is newer; treat NULL stored as older
    -- session_id           = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(session_id),           session_id),
    -- session_name         = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(session_name),         session_name),
    -- date_modified        = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(date_modified),        date_modified),
    -- execution_status     = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(execution_status),     execution_status),
    -- ica_link             = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(ica_link),             ica_link),
    -- ica_project_id       = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(ica_project_id),       ica_project_id),
    -- workflow_reference   = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(workflow_reference),   workflow_reference),
    -- run_id               = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(run_id),               run_id),
    -- run_name             = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(run_name),             run_name),
    -- percent_gt_q30       = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(percent_gt_q30),       percent_gt_q30),
    -- flowcell_barcode     = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(flowcell_barcode),     flowcell_barcode),
    -- reagent_barcode      = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(reagent_barcode),      reagent_barcode),
    -- status               = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(status),               status),
    -- experiment_name      = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(experiment_name),      experiment_name),
    -- run_date_created     = GREATEST(run_date_created, VALUES(run_date_created)),
    -- biosample_id         = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(biosample_id),         biosample_id),
    -- computed_yield_bps   = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(computed_yield_bps),   computed_yield_bps),
    -- generated_sample_id  = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, VALUES(generated_sample_id),  generated_sample_id),
    -- updated_at           = IF(run_date_created IS NULL OR VALUES(run_date_created) > run_date_created, NOW(),                        updated_at);


