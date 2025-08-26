INSERT INTO illumina_bssh_biosamples (
    session_id
    , session_name
    , date_created
    , run_name
    , experiment_name
    , run_date_created
    , biosample_name
    , biosample_id
    , computed_yield_bps
    , generated_sample_id
    , created_at
    , updated_at
)

VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

ON DUPLICATE KEY UPDATE
session_id = VALUES(session_id)
, session_name = VALUES(session_name)
, date_created = VALUES(date_created)
, run_name = VALUES(run_name)
, experiment_name = VALUES(experiment_name)
, run_date_created = VALUES(run_date_created)
, biosample_name = VALUES(biosample_name)
, biosample_id = VALUES(biosample_id)
, computed_yield_bps = VALUES(computed_yield_bps)
, generated_sample_id = VALUES(generated_sample_id)
, updated_at = '{{ ts }}'