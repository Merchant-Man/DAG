INSERT INTO illumina_bssh_runs (
    session_id
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
    , id_library
    , run_date_created
    , total_flowcell_yield_Gbps
    , created_at
    , updated_at
)

VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

ON DUPLICATE KEY UPDATE
session_id = VALUES(session_id)
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
, id_library = VALUES(id_library)
, run_date_created = VALUES(run_date_created)
, total_flowcell_yield_Gbps = VALUES(total_flowcell_yield_Gbps)
, updated_at = '{{ ts }}'