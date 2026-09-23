-- PURPOSE: Fetch the highest sequence number seen so far, per region.
-- Seeds the in-memory sequence baseline for Task 1 check 8. Only files
-- that were successfully VALIDATED count toward the baseline -- a
-- rejected file must not block a later legitimate sequence number.
SELECT
    region,
    MAX(sequence_number) AS max_sequence_number
FROM ${DORIS_DATABASE}.file_processing_log
WHERE validation_status = 'VALIDATED'
  AND region IS NOT NULL
GROUP BY region;
