-- Raw ingestion table for validated Actives SIM records.
-- Grain: one row per unique ICCID.
-- Column names, types, and the UNIQUE KEY are exactly as specified by the
-- assessment DDL. The one deviation: `iccid` is declared as the FIRST
-- column instead of its original 21st position. Real Doris rejects the
-- assessment's literal column order outright --
--   errCode = 2, detailMessage = Key columns should be a ordered prefix
--   of the schema. KeyColumns[0] (starts from zero) is iccid, but
--   corresponding column is file_name in the previous columns
--   declaration.
-- Doris requires UNIQUE KEY column(s) to be a leading prefix of the
-- physical column declaration order. This is a required technical
-- adjustment (column order only -- no name, type, or key change), not a
-- reinterpretation of the schema. Confirmed against the real Doris
-- cluster (database_ganesh). Rows are written via Stream Load
-- (src/clients/doris_client.py:stream_load), which names columns
-- explicitly in its `columns` header, so this reordering has no effect
-- on how rows are written or read.
CREATE TABLE IF NOT EXISTS ${DORIS_DATABASE}.raw_actives_daily (
    iccid                           VARCHAR(32) NOT NULL,
    file_name                      VARCHAR(255),
    companyid                      VARCHAR(64),
    companyname                    VARCHAR(255),
    invoicenumber                  VARCHAR(64),
    businessunitname               VARCHAR(255),
    departmentname                 VARCHAR(255),
    costcentername                 VARCHAR(255),
    commercialoffername            VARCHAR(255),
    businessunitlevel              VARCHAR(10),
    businessunittag                VARCHAR(255),
    sharedbalanceused              VARCHAR(50),
    accountnumber                  VARCHAR(64),
    customernumber                 VARCHAR(64),
    msisdn                         VARCHAR(32),
    productstatus                  VARCHAR(50),
    provproducttype                VARCHAR(100),
    activationdate                 VARCHAR(30),
    lastproductstatuschangedate    VARCHAR(30),
    commercialofferdescription     VARCHAR(255),
    lasttrafficdate                VARCHAR(30),
    productid                      VARCHAR(64),
    extramsisdn1                   VARCHAR(32),
    extracreationdate1             VARCHAR(30),
    extramsisdn2                   VARCHAR(32),
    extracreationdate2             VARCHAR(30),
    extramsisdn3                   VARCHAR(32),
    extracreationdate3             VARCHAR(30),
    extramsisdn4                   VARCHAR(32),
    extracreationdate4             VARCHAR(30),
    planname                       VARCHAR(255),
    planstartdate                  VARCHAR(30),
    carriername                    VARCHAR(255),
    cdpplatform                    VARCHAR(100),
    carrieraccountno               VARCHAR(64),
    providername                   VARCHAR(255),
    `type`                         VARCHAR(50),
    profileversion                 VARCHAR(50),
    rateplanname                   VARCHAR(255),
    rateplancodeid                 VARCHAR(64),
    zonetype                       VARCHAR(50),
    whitelistedzones               VARCHAR(500),
    blacklistedzones               VARCHAR(500),
    simbatch                       VARCHAR(100),
    simvendor                      VARCHAR(100),
    simtype                        VARCHAR(50),
    eid                            VARCHAR(64),
    loaded_at                      DATETIME
)
UNIQUE KEY(iccid)
DISTRIBUTED BY HASH(iccid) BUCKETS 8
PROPERTIES ("replication_num" = "1");
