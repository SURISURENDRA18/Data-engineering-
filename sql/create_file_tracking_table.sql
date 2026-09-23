-- File tracking table for idempotency, sequence checks, and operational monitoring
CREATE TABLE IF NOT EXISTS {database_name}.actives_file_tracking (
    file_hash VARCHAR(64) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    region VARCHAR(64),
    sequence_number BIGINT,
    status VARCHAR(32) NOT NULL,
    rows_loaded BIGINT DEFAULT 0,
    rows_rejected BIGINT DEFAULT 0,
    processed_at DATETIME,
    error_message TEXT
)
UNIQUE KEY(file_hash)
DISTRIBUTED BY HASH(file_hash) BUCKETS 4
PROPERTIES ("replication_num" = "1");
