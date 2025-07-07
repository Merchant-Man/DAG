-- Load BCLConvert session data
INSERT INTO illumina_appsessions (
row_type,session_id,session_name,date_created,run_name,experiment_name,run_date_created,biosaample_name,biosample_id,computed_yield_bp,generated_sample_id, created_at, updated_at
) VALUES (
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
ON DUPLICATE KEY UPDATE
row_type=VALUES(row_type),session_id=VALUES(session_id),session_name=VALUES(session_name),date_created=VALUES(date_created),run_name=VALUES(run_name),experiment_name=VALUES(experiment_name),run_date_created=VALUES(run_date_created),biosaample_name=VALUES(biosaample_name),biosample_id=VALUES(biosample_id),computed_yield_bp=VALUES(computed_yield_bp),generated_sample_id=VALUES(generated_sample_id), created_at=VALUES(created_at), updated_at=VALUES(updated_at);
