INSERT INTO phenovar_digital_consent(
    id_subject,
    is_interview,
    is_taking_blood_sample,
    is_taking_blood_network,
    is_taking_additional_test,
    is_access_record_to_medical_record,
    is_data_exchanged_with_other_researcher,
    is_family_have_access_sample,
    is_know_result_researcher,
    ratification_date,
    withness,
    created_by,
    updated_by,
    creation_date,
    updation_date
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE 
is_interview = VALUES(is_interview),
is_taking_blood_sample = VALUES(is_taking_blood_sample),
is_taking_blood_network = VALUES(is_taking_blood_network),
is_taking_additional_test = VALUES(is_taking_additional_test),
is_access_record_to_medical_record = VALUES(is_access_record_to_medical_record),
is_data_exchanged_with_other_researcher = VALUES(is_data_exchanged_with_other_researcher),
is_family_have_access_sample = VALUES(is_family_have_access_sample),
is_know_result_researcher = VALUES(is_know_result_researcher),
ratification_date = VALUES(ratification_date),
withness = VALUES(withness),
updated_by = VALUES(updated_by),
updation_date = VALUES(updation_date)
;
