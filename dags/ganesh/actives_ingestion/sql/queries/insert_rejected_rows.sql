-- PURPOSE: Bulk-insert row-level rejects (Task 2 rule 6) into the audit
-- table. Executed with executemany() by the rejected-row loader.
INSERT INTO ${DORIS_DATABASE}.raw_actives_daily_rejected (
    file_name,
    row_number,
    iccid,
    customernumber,
    companyid,
    reject_reason,
    raw_row_json,
    rejected_at
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s
);
