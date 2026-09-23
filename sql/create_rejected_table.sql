-- Rejected rows table for debugging and auditing
CREATE TABLE IF NOT EXISTS {database_name}.raw_actives_daily_rejected (
    id VARCHAR(64) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    row_number BIGINT NOT NULL,
    reason VARCHAR(64) NOT NULL,
    iccid VARCHAR(64),
    customernumber VARCHAR(64),
    companyid VARCHAR(64),
    raw_row TEXT,
    rejected_at DATETIME
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");
