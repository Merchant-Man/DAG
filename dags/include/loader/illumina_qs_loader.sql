INSERT INTO illumina_qs(
    id_repository
    , lane
    , `index`
    , index2
    , read_number
    , yield
    , yield_q30
    , quality_score_sum
    , mean_quality_score_pf
    , percent_q30
    , created_at
    , updated_at
    , id_library
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE 
id_repository = VALUES(id_repository)
, lane = VALUES(lane)
, `index` = VALUES(`index`)
, index2 = VALUES(index2)
, read_number = VALUES(read_number)
, yield = VALUES(yield)
, yield_q30 = VALUES(yield_q30)
, quality_score_sum = VALUES(quality_score_sum)
, mean_quality_score_pf = VALUES(mean_quality_score_pf)
, percent_q30 = VALUES(percent_q30)
, updated_at = '{{ ts }}'
, id_library=VALUES(id_library)