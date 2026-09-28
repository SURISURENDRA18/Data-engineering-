# Actives Ingestion Pipeline — Data Engineering Assessment

A production-grade, highly reliable, idempotent, and non-destructive two-stage Apache Airflow ingestion pipeline.
This pipeline processes telecom active subscriber daily feeds from MinIO object storage, conducts rigorous validation, routes files to structured object storage paths, and loads valid subscriber records into Apache Doris with exact row-level deduplication and comprehensive tracking.

---

## 1. Project Architecture

The pipeline follows a decoupled two-stage architecture orchestrated by Apache Airflow:

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        STAGE 1: FILE VALIDATION                        │
│                           (validate_and_reject)                        │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                      crushftp/mediation/landing/
                                    │
                      Checks 1-8 (First-failure wins)
                                    ├─── PASS ──► crushftp/mediation/surendra/validated/
                                    │
                                    └─── FAIL ──► crushftp/mediation/surendra/rejected/<reason>/
                                    │
                       (Source landing files permanently preserved)
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         STAGE 2: DATA LOADING                          │
│                            (load_raw_table)                            │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                   crushftp/mediation/surendra/validated/
                                    │
                         Idempotency Hash Check
                                    │
                         Row-Level Validation
                                    │
                                    ├─── FAIL ──► surendra_db.raw_actives_daily_rejected
                                    │
                                    ▼
                         Row Deduplication (Latest wins)
                                    │
                                    ▼
                         Stream Load / Batch Insert
                                    │
                                    ├───► surendra_db.raw_actives_daily
                                    │
                                    └───► surendra_db.actives_file_tracking (SUCCESS)
                                    │
                      (Validated MinIO files permanently preserved)
```

### DAG Structure & Dependencies

- **DAG ID**: `actives_ingestion_dag` (or dedicated candidate DAG `surendra_actives_ingestion`)
- **Strict Dependency**:
  ```text
  validate_and_reject >> load_raw_table
  ```

---

## 2. Prerequisites

- **Python**: Version 3.11 or newer (tested on Python 3.11.15)
- **Object Storage**: MinIO API (S3-compatible) running and reachable
- **Data Warehouse**: Apache Doris (MySQL frontend port and HTTP Stream Load port reachable)
- **Orchestrator**: Apache Airflow 3.x with REST API and JWT Authentication
- **Python Dependencies**: Listed in `requirements.txt`:
  - `minio>=7.2.0`
  - `pymysql>=1.1.0`
  - `requests>=2.31.0`
  - `python-dotenv>=1.0.0`
  - `pytest>=8.0.0`
  - `ruff>=0.3.0`

---

## 3. Environment Variables

Create or configure a `.env` file in the project root (never committed to version control):

```env
# MinIO Object Storage
MINIO_ENDPOINT=34.130.209.7:32010
MINIO_ACCESS_KEY=ZUHn0xWNRcrW9j72N9dk
MINIO_SECRET_KEY=F5KIECDZgAuOLVTLoK6eKhPQKKRl6xTBfsX0ARc7
MINIO_SECURE=false
MINIO_BUCKET=crushftp
MINIO_LANDING_PREFIX=mediation/landing/
MINIO_VALIDATED_PREFIX=mediation/surendra/validated/
MINIO_REJECTED_PREFIX=mediation/surendra/rejected/

# Apache Doris
DORIS_HOST=34.130.209.7
DORIS_PORT=31545
DORIS_HTTP_PORT=32713
DORIS_USER=root
DORIS_PASSWORD=
DORIS_DATABASE=surendra_db
DORIS_RAW_TABLE=raw_actives_daily
DORIS_REJECTED_TABLE=raw_actives_daily_rejected
DORIS_TRACKING_TABLE=actives_file_tracking
DORIS_CONNECT_TIMEOUT=10
BATCH_SIZE=5000

# Remote Apache Airflow 3
AIRFLOW_HOST=34.130.209.7
AIRFLOW_PORT=32002
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_VERSION=3.3.2
AIRFLOW_UI_URL=http://34.130.209.7:32002
AIRFLOW_API_URL=http://34.130.209.7:32002/api/v2
```

---

## 4. MinIO Configuration

- **Bucket**: `crushftp`
- **Source Landing Path**: `mediation/landing/`
- **Validated Output Path**: `mediation/surendra/validated/`
- **Rejected Output Path**: `mediation/surendra/rejected/<reason>/`
  - Rejection subdirectories:
    - `pattern_mismatch/`
    - `zero_size/`
    - `header_only/`
    - `not_text_format/`
    - `invalid_column_count/`
    - `duplicate/`
    - `sequence_invalid/`

---

## 5. Doris Configuration

The pipeline requires three persistent tables in `surendra_db`:

### 1. Raw Table: `surendra_db.raw_actives_daily`
Stores valid, deduplicated subscriber records with metadata.
```sql
CREATE TABLE IF NOT EXISTS surendra_db.raw_actives_daily (
    customernumber VARCHAR(64) NOT NULL,
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
UNIQUE KEY(customernumber, iccid)
DISTRIBUTED BY HASH(iccid) BUCKETS 8
PROPERTIES ("replication_num" = "1");
```

### 2. Rejected Rows Table: `surendra_db.raw_actives_daily_rejected`
Stores individual malformed rows with row numbers, raw line contents, and rejection reason for auditability.
```sql
CREATE TABLE IF NOT EXISTS surendra_db.raw_actives_daily_rejected (
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
```

### 3. File Tracking Table: `surendra_db.actives_file_tracking`
Tracks file processing state, SHA-256 identity, region sequence, and metrics.
```sql
CREATE TABLE IF NOT EXISTS surendra_db.actives_file_tracking (
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
```

---

## 6. Airflow Setup

Airflow 3 connects via Airflow Connections or environment variables:
- `minio_default`: MinIO endpoint, access key, secret key.
- `doris_default`: Doris MySQL host, port, credentials.
- The pipeline supports Airflow 3 token-based REST authentication (`/auth/token`) and query endpoints (`/api/v2/dags`, `/api/v2/monitor/health`).

---

## 7. How to Start the Services & Diagnostic Verification

Verify connectivity to all external services using the read-only diagnostic script:

```bash
python3 scripts/verify_all_connections.py
```

This verifies:
- Airflow UI & REST API health (34.130.209.7:32002)
- Doris Coupled MySQL client (34.130.209.7:31545)
- Doris Coupled Stream Load (34.130.209.7:32713)
- MinIO UI (34.130.209.7:30957)
- MinIO API (34.130.209.7:32010)

---

## 8. How to Trigger the DAG

### Option A: Dedicated Live Airflow CLI Test Runner
```bash
python3 scripts/test_live_airflow.py
# Or with custom options:
python3 scripts/test_live_airflow.py --timeout 150
python3 scripts/test_live_airflow.py --skip-staging --filter "Active"
```

### Option B: Via Airflow REST API / CLI Diagnostic
```bash
python3 scripts/test_airflow_connection.py
```
Or via curl:
```bash
TOKEN=$(curl -s -X POST http://34.130.209.7:32002/auth/token -H "Content-Type: application/json" -d '{"username":"admin","password":"admin"}' | jq -r .access_token)
curl -X POST http://34.130.209.7:32002/api/v2/dags/surendra_actives_ingestion/dagRuns \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"conf":{}}'
```

### Option C: Unified Pipeline Runner

```bash
# Run safe end-to-end demo with test fixtures
python3 scripts/run_pipeline.py --mode fixtures

# Run against existing MinIO landing files
python3 scripts/run_pipeline.py --mode landing

# Run with custom file filter
python3 scripts/run_pipeline.py --mode landing --filter "Active"
```

### Option D: Direct Local DAG Execution
```bash
python3 dags/surendra/surendra_actives_ingestion_dag.py
```


---

## 9. Task 1 Behavior: Validation and Routing

Task 1 (`validate_and_reject`) scans `crushftp/mediation/landing/` and evaluates each file against **8 strict checks in exact order (First Failure Wins)**:

| Order | Check | Condition / Rule | Reject Reason / Destination |
| :---: | :--- | :--- | :--- |
| **1** | **Filename Pattern** | Must match `^[A-Za-z]+_[0-9]+_(Active\|Actives)_[0-9]+_[0-9]+\.csv(\.gz)?$` with 14-digit timestamp | `rejected/pattern_mismatch/` |
| **2** | **Zero Size** | File size must be strictly > 0 bytes | `rejected/zero_size/` |
| **3** | **Header Only** | File must contain at least one data record below header (works for `.csv` and `.csv.gz`) | `rejected/header_only/` |
| **4** | **Text / Corruption** | Readable text; valid gzip decompressibility; no binary null bytes (`\x00`) in text lines | `rejected/not_text_format/` |
| **5** | **Header 46 Columns** | Exactly 46 columns in exact required order (`CompanyID` to `EID`) | `rejected/invalid_column_count/` |
| **6** | **Data Row Columns** | Every row must contain exactly 46 fields; handles quoted delimiters (`"Acme; Inc"`) safely | `rejected/invalid_column_count/` |
| **7** | **Duplicate File** | SHA-256 content hash check against in-batch seen files and persistent tracking history | `rejected/duplicate/` |
| **8** | **Sequence Validation**| Sequence numbers must be positive and strictly increasing per region (`seq > max_processed`) | `rejected/sequence_invalid/` |

---

## 10. Task 2 Behavior: Row Validation and Loading

Task 2 (`load_raw_table`) scans `crushftp/mediation/surendra/validated/` and processes each file independently:

1. **File Idempotency Check**: Computes SHA-256 hash. If status is `SUCCESS` in `actives_file_tracking`, skips immediately.
2. **Row Validation**: Evaluates each record in strict priority order:
   - ICCID is empty or null -> `NULL_OR_EMPTY_ICCID`
   - CustomerNumber is empty or null -> `NULL_OR_EMPTY_CUSTOMER_NUMBER`
   - CompanyID is empty, null, or zero -> `NULL_OR_ZERO_COMPANY_ID`
   - Invalid rows are routed to `raw_actives_daily_rejected` without aborting the file.
3. **Row Deduplication**: When duplicate `(CustomerNumber, ICCID)` pairs exist within a file, the **latest row in source order wins**; earlier rows are dropped.
4. **Metadata Attachment**: Every raw row receives `file_name` and UTC `loaded_at`.
5. **Bulk Ingestion**: Rows are loaded via HTTP Stream Load with automatic fallback to batched MySQL INSERT (`executemany`).
6. **State Tracking**: Writes `status = 'SUCCESS'`, loaded row count, and rejected row count to `actives_file_tracking`.

---

## 11. Idempotency Design

- **SHA-256 Hashing**: File identity is based on cryptographic SHA-256 hash of object bytes, guaranteeing that renames or re-runs cannot bypass duplicate detection.
- **Tracking Table**: `actives_file_tracking` tracks `file_hash` as a UNIQUE KEY.
- **Re-run Safety**:
  - Task 1: If destination already exists in `validated/` or `rejected/`, it is safely skipped.
  - Task 2: If `file_hash` has status `SUCCESS`, it is skipped (`0` rows loaded, `0` rows added).
  - Database row counts remain strictly identical across re-runs.

---

## 12. Duplicate Handling

- **File-Level**: Duplicate files (identical SHA-256 content) are detected at Check 7 and routed to `rejected/duplicate/`.
- **Row-Level**: Within a validated file, records sharing the business key pair `(CustomerNumber, ICCID)` are deduplicated in memory prior to Doris ingestion, keeping the latest occurrence. Doris table schema also enforces `UNIQUE KEY(customernumber, iccid)` with merge-on-write semantics.

---

## 13. Rejected-Row Handling

- Malformed rows do **not** abort file loading (Per-File and Per-Row Isolation).
- Rejected rows are persisted to `surendra_db.raw_actives_daily_rejected` with:
  - `id`: Unique UUID
  - `file_name`: Source file name
  - `file_hash`: Source SHA-256 hash
  - `row_number`: 1-indexed row number in file
  - `reason`: Exact reject reason code
  - `iccid`, `customernumber`, `companyid`: Extracted fields
  - `raw_row`: Full raw text line
  - `rejected_at`: UTC timestamp

---

## 14. Sequence Validation Assumption

- **Per-Region Tracking**: Sequence validation is evaluated independently per region (extracted from filename regex group `region`).
- **Strictly Increasing Rule**:
  - Sequence numbers must be integers $\ge 1$.
  - Sequences must not repeat for the same region.
  - New sequence number must be strictly greater than the maximum sequence number previously successfully processed for that region.
  - Files with out-of-order sequences are routed to `rejected/sequence_invalid/`.

---

## 15. File Preservation / Non-Destructive Design (CRITICAL)

**NON-NEGOTIABLE SAFETY POLICY**:
- **NEVER DELETE SOURCE DATA**: MinIO landing files are **never** deleted, removed, truncated, or overwritten.
- **Copy-Only Routing**: File routing is strictly implemented via `client.copy_object(...)`. The source file remains in `crushftp/mediation/landing/` permanently.
- **Validated Object Preservation**: Validated files in `crushftp/mediation/surendra/validated/` remain permanently for auditability and replay.
- **No SQL Deletion**: No `DELETE`, `TRUNCATE`, or `DROP` statements exist in the ETL execution path.

---

## 16. How to Verify Results

Run read-only inspection queries against Doris:

```sql
-- Total loaded subscribers
SELECT COUNT(*) FROM surendra_db.raw_actives_daily;

-- Total rejected rows
SELECT COUNT(*) FROM surendra_db.raw_actives_daily_rejected;

-- File tracking records
SELECT file_name, file_hash, status, rows_loaded, rows_rejected, processed_at
FROM surendra_db.actives_file_tracking
ORDER BY processed_at DESC;

-- Row count per ingested file
SELECT file_name, COUNT(*) AS row_count
FROM surendra_db.raw_actives_daily
GROUP BY file_name
ORDER BY file_name;

-- Audit breakdown of rejected rows by reason
SELECT file_name, reason, COUNT(*) AS rejected_count
FROM surendra_db.raw_actives_daily_rejected
GROUP BY file_name, reason
ORDER BY file_name, reason;
```

---

## 17. Expected DAG & Task Names

- **Airflow DAG ID**: `actives_ingestion_dag` (or candidate-specific `surendra_actives_ingestion`)
- **Task 1 ID**: `validate_and_reject`
- **Task 2 ID**: `load_raw_table`

---

## 18. Testing & Verification

Run the comprehensive automated test suite:

```bash
# 1. Run all unit tests (61 tests covering filename, format, schema, sequence, deduplication, row validation)
python3 -m pytest tests/unit -v

# 2. Run pipeline hermetic integration tests
python3 -m pytest tests/integration/test_pipeline.py -v

# 3. Run live service integration tests against live MinIO and Doris
python3 -m pytest tests/integration/test_live_services.py -v

# 4. Run live Airflow 3 end-to-end integration tests (health, DAG status, remote trigger, log retrieval, warehouse verification)
python3 -m pytest tests/integration/test_live_airflow.py -v

# 5. Standalone Live Airflow CLI test runner
python3 scripts/test_live_airflow.py

# 6. Code quality & linting
ruff check src dags/surendra tests scripts
```

