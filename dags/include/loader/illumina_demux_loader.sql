INSERT INTO illumina_demux (
    id_repository
    , lane
    , index_seq
    , total_reads
    , total_perfect_index_reads
    , total_one_mismatch_index_reads
    , total_two_mismatch_index_reads
    , percent_reads
    , percent_perfect_index_reads
    , percent_one_mismatch_index_reads
    , percent_two_mismatch_index_reads
    , id_library
    , created_at
    , updated_at
)

VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

ON DUPLICATE KEY UPDATE
id_repository = VALUES(id_repository)
, lane = VALUES(lane)
, index_seq = VALUES(index_seq)
, total_reads = VALUES(total_reads)
, total_perfect_index_reads = VALUES(total_perfect_index_reads)
, total_one_mismatch_index_reads = VALUES(total_one_mismatch_index_reads)
, total_two_mismatch_index_reads = VALUES(total_two_mismatch_index_reads)
, percent_reads = VALUES(percent_reads)
, percent_perfect_index_reads = VALUES(percent_perfect_index_reads)
, percent_one_mismatch_index_reads = VALUES(percent_one_mismatch_index_reads)
, percent_two_mismatch_index_reads = VALUES(percent_two_mismatch_index_reads)
, id_library = VALUES(id_library)
, updated_at = '{{ ts }}'