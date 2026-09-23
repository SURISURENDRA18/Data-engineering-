-- Row-level rejection audit log for Task 2 loading.
-- Grain: one row per rejected source row. Append-only diagnostic table --
-- not authoritative (the raw table is authoritative and self-heals via
-- UNIQUE KEY(iccid)), so no natural unique key is enforced here.
CREATE TABLE IF NOT EXISTS ${DORIS_DATABASE}.raw_actives_daily_rejected (
    file_name       VARCHAR(255),
    row_number      INT,
    iccid           VARCHAR(32),
    customernumber  VARCHAR(64),
    companyid       VARCHAR(64),
    reject_reason   VARCHAR(50),
    raw_row_json    STRING,
    rejected_at     DATETIME
)
DUPLICATE KEY(file_name, row_number)
DISTRIBUTED BY HASH(file_name) BUCKETS 8
PROPERTIES ("replication_num" = "1");
