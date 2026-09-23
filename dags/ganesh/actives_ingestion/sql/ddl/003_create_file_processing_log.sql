-- Single shared control table driving idempotency for both pipeline
-- stages: Task 1's duplicate check (#7) and sequence check (#8) read
-- from it; Task 2's load idempotency reads/writes load_status on it.
-- Grain: one row per distinct file content (file_checksum).
CREATE TABLE IF NOT EXISTS ${DORIS_DATABASE}.file_processing_log (
    file_checksum                  VARCHAR(64) NOT NULL,
    file_name                      VARCHAR(255) NOT NULL,
    region                         VARCHAR(50),
    sequence_number                INT,
    landing_key                    VARCHAR(1000),
    current_key                    VARCHAR(1000),
    validation_status              VARCHAR(20),
    rejection_reason               VARCHAR(50),
    validation_detail              VARCHAR(1000),
    load_status                    VARCHAR(20),
    row_count_total                INT,
    row_count_loaded               INT,
    row_count_rejected             INT,
    row_count_duplicate_dropped    INT,
    file_size_bytes                BIGINT,
    validated_at                   DATETIME,
    loaded_at                      DATETIME,
    updated_at                     DATETIME
)
UNIQUE KEY(file_checksum)
DISTRIBUTED BY HASH(file_checksum) BUCKETS 8
PROPERTIES ("replication_num" = "1");
