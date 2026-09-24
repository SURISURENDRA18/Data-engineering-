USE santhosh_actives_db;

-- 1. Drop the old table with incorrect columns
DROP TABLE IF EXISTS santhosh_actives_db.raw_actives;



-- 2. Create raw_actives with iccid as the leading key column
CREATE TABLE santhosh_actives_db.raw_actives (
    iccid VARCHAR(32) NOT NULL,
    file_name VARCHAR(255),
    companyid VARCHAR(64),
    companyname VARCHAR(255),
    invoicenumber VARCHAR(64),
    businessunitname VARCHAR(255),
    departmentname VARCHAR(255),
    costcentername VARCHAR(255),
    commercialoffername VARCHAR(255),
    businessunitlevel VARCHAR(10),
    businessunittag VARCHAR(255),
    sharedbalanceused VARCHAR(50),
    accountnumber VARCHAR(64),
    customernumber VARCHAR(64),
    msisdn VARCHAR(32),
    productstatus VARCHAR(50),
    provproducttype VARCHAR(100),
    activationdate VARCHAR(30),
    lastproductstatuschangedate VARCHAR(30),
    commercialofferdescription VARCHAR(255),
    lasttrafficdate VARCHAR(30),
    productid VARCHAR(64),
    extramsisdn1 VARCHAR(32),
    extracreationdate1 VARCHAR(30),
    extramsisdn2 VARCHAR(32),
    extracreationdate2 VARCHAR(30),
    extramsisdn3 VARCHAR(32),
    extracreationdate3 VARCHAR(30),
    extramsisdn4 VARCHAR(32),
    extracreationdate4 VARCHAR(30),
    planname VARCHAR(255),
    planstartdate VARCHAR(30),
    carriername VARCHAR(255),
    cdpplatform VARCHAR(100),
    carrieraccountno VARCHAR(64),
    providername VARCHAR(255),
    `type` VARCHAR(50),
    profileversion VARCHAR(50),
    rateplanname VARCHAR(255),
    rateplancodeid VARCHAR(64),
    zonetype VARCHAR(50),
    whitelistedzones VARCHAR(500),
    blacklistedzones VARCHAR(500),
    simbatch VARCHAR(100),
    simvendor VARCHAR(100),
    simtype VARCHAR(50),
    eid VARCHAR(64),
    loaded_at DATETIME
)
UNIQUE KEY(iccid)
DISTRIBUTED BY HASH(iccid) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 3. Create the rejected rows table
CREATE TABLE IF NOT EXISTS santhosh_actives_db.raw_actives_rejected (
    file_name VARCHAR(255) NOT NULL,
    iccid VARCHAR(32) NOT NULL,
    customernumber VARCHAR(64),
    companyid VARCHAR(64),
    reject_reason VARCHAR(100),
    rejected_at DATETIME
)
DUPLICATE KEY(file_name, iccid)
DISTRIBUTED BY HASH(iccid) BUCKETS 2
PROPERTIES ("replication_num" = "1");

-- 4. Create the audit table for idempotency
CREATE TABLE IF NOT EXISTS santhosh_actives_db.loaded_files_audit (
    file_name VARCHAR(255) NOT NULL,
    records_loaded INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    loaded_at DATETIME
)
UNIQUE KEY(file_name)
DISTRIBUTED BY HASH(file_name) BUCKETS 2
PROPERTIES ("replication_num" = "1");

-- 1. Check total loaded rows (should match the 255,000 from the logs)
SELECT count(*) AS total_raw_rows FROM santhosh_actives_db.raw_actives;

-- 2. Check rejected rows
SELECT count(*) AS total_rejected_rows FROM santhosh_actives_db.raw_actives_rejected;

-- 3. Check the audit log to ensure all processed files were tracked
SELECT count(*) AS total_audited_files FROM santhosh_actives_db.loaded_files_audit;

SHOW TABLES FROM santhosh_actives_db;