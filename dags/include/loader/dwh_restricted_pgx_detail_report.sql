INSERT INTO dwh_restricted.gold_pgx_detail_report(order_id,hubs,drug_category,drug_name,drug_classification,phenotype_text,gene_symbol,branded_drug,nala_score_v2,rec_category,rec_source,rec_text,run_id,scientific_evidence_symbol,implication_text,test_method,`version`)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
hubs = VALUES(hubs),
drug_category = VALUES(drug_category),
drug_name = VALUES(drug_name),
drug_classification = VALUES(drug_classification),
phenotype_text = VALUES(phenotype_text),
gene_symbol = VALUES(gene_symbol),
branded_drug = VALUES(branded_drug),
nala_score_v2 = VALUES(nala_score_v2),
rec_category = VALUES(rec_category),
rec_source = VALUES(rec_source),
rec_text = VALUES(rec_text),
run_id = VALUES(run_id),
scientific_evidence_symbol = VALUES(scientific_evidence_symbol),
implication_text = VALUES(implication_text),
test_method = VALUES(test_method),
`version` = VALUES(`version`)