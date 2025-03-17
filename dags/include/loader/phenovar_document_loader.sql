INSERT INTO phenovar_documents(participant_id,institution_id,institution_name,document_type,question_answer,user,`version`,created_by,created_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE institution_id=VALUES(institution_id),
institution_name=VALUES(institution_name),
document_type=VALUES(document_type),
question_answer=VALUES(question_answer),
user=VALUES(user),
`version`=VALUES(`version`),
created_at=VALUES(created_at)