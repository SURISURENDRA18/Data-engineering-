-- PURPOSE: Look up whether a file (by content checksum) has any existing
-- tracking row, regardless of its prior status. Used by:
--   * Task 1 check 7 (duplicate): any row at all => duplicate.
--   * Task 2 load idempotency: inspect load_status to decide skip/retry.
-- Selects every column (not just the subset each caller needs) because
-- record_load_result() performs a read-modify-write over the full row --
-- Doris's UNIQUE KEY(file_checksum) replaces the whole row on INSERT, so
-- any column left out here would be silently blanked on that rewrite.
SELECT
    file_checksum,
    file_name,
    region,
    sequence_number,
    landing_key,
    current_key,
    validation_status,
    rejection_reason,
    validation_detail,
    load_status,
    row_count_total,
    row_count_loaded,
    row_count_rejected,
    row_count_duplicate_dropped,
    file_size_bytes,
    validated_at,
    loaded_at
FROM ${DORIS_DATABASE}.file_processing_log
WHERE file_checksum = %s
LIMIT 1;
