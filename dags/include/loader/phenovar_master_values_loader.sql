INSERT INTO phenovar_master_values (
  id, category_name, input_type, value_have_poin, answer, created_by, updated_by, created_at, updated_at
)
VALUES (%s, %s, %s, %s, CAST(%s AS JSON), %s, %s, %s, %s) AS new
ON DUPLICATE KEY UPDATE
  category_name   = new.category_name,
  input_type      = new.input_type,
  value_have_poin = new.value_have_poin,
  answer          = new.answer,
  created_by      = new.created_by,
  updated_by      = new.updated_by,
  updated_at      = new.updated_at;
