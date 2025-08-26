INSERT INTO illumina_demux (
    id_repository
    , lane
    , `index`
    , reads
    , perfect_index_reads
    , one_mismatch_index_reads
    , two_mismatch_index_reads
    , percent_reads
    , percent_perfect_index_reads
    , percent_one_mismatch_index_reads
    , percent_two_mismatch_index_reads
    , id_library
    , created_at
    , updated_at
)

VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

ON DUPLICATE KEY UPDATE
id_repository = VALUES(id_repository)
, lane = VALUES(lane)
, `index` = VALUES(`index`)
, reads = VALUES(reads)
, perfect_index_reads = VALUES(perfect_index_reads)
, one_mismatch_index_reads = VALUES(one_mismatch_index_reads)
, two_mismatch_index_reads = VALUES(two_mismatch_index_reads)
, percent_reads = VALUES(percent_reads)
, percent_perfect_index_reads = VALUES(percent_perfect_index_reads)
, percent_one_mismatch_index_reads = VALUES(percent_one_mismatch_index_reads)
, percent_two_mismatch_index_reads = VALUES(percent_two_mismatch_index_reads)
, id_library = VALUES(id_library)
, updated_at = '{{ ts }}'