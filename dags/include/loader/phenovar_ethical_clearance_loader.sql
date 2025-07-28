INSERT INTO phenovar_ethical_clearance(
    id,
    project_id,
    institution_id,
    institution_name,
    hub_id,
    hub_name,
    ec_number,
    effective_date,
    `expiry_date`,
    research_title,
    pic,
    `description`,
    created_by,
    updated_by,
    creation_date,
    updation_date,
    created_at,
    updated_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE 
project_id=VALUES(project_id),institution_id=VALUES(institution_id),institution_name=VALUES(institution_name),hub_id=VALUES(hub_id),hub_name=VALUES(hub_name),ec_number=VALUES(ec_number),effective_date=VALUES(effective_date),`expiry_date`=VALUES(`expiry_date`),research_title=VALUES(research_title),pic=VALUES(pic),`description`=VALUES(`description`),created_by=VALUES(created_by),updated_by=VALUES(updated_by),creation_date=VALUES(creation_date),updation_date=VALUES(updation_date),updated_at=VALUES(updated_at)