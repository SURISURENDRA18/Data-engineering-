-- PURPOSE: Write (or replace) the full tracking row for one file.
-- Relies on file_processing_log's UNIQUE KEY(file_checksum): Doris
-- replaces any existing row for this checksum on INSERT, so both
-- record_validation_result() and record_load_result() simply re-insert
-- the complete row rather than issuing a partial UPDATE.
INSERT INTO ${DORIS_DATABASE}.file_processing_log (
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
    loaded_at,
    updated_at
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);
