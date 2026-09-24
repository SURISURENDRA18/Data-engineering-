# Data Engineering Assessment: Actives Ingestion Pipeline

## Overview
This assessment implements an automated, two-stage data ingestion pipeline in Apache Airflow for daily regional active SIM records (Actives feed):

* **Task 1 (`validate_and_reject`):** Scans incoming files from the landing area (`crushftp/mediation/landing/`), checks each file sequentially against 8 strict file-level validation rules, and moves valid files to `mediation/validated/` while isolating invalid files into `mediation/rejected/<reason>/.

* **Task 2 (`load_raw_table`):** Reads validated files, enforces row-level data quality constraints (required fields and rejection reasons), deduplicates identical `(iccid, customernumber)` pairs, and loads clean records into Apache Doris (`raw_actives`) while routing bad records to `raw_actives_rejected` and ensuring strict idempotency through `loaded_files_audit`.


---

## 1. Architecture Overview

```text
                               AIRFLOW (Orchestrator)
                             [ actives_ingestion_dag.py ]
                                          │
                                          ▼
                      ╔═══════════════════════════════════════╗
                      ║     STAGE 1: FILE VALIDATION ENGINE   ║
                      ║        (task1_file_validator)         ║
                      ╚═══════════════════════════════════════╝
                                          │
                            [ MinIO: mediation/landing ]
                                          │
                                          ▼
                              src/file_validator.py
                           (8-Rule Validation Engine)
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    ▼                                           ▼
          [ FAILED VALIDATION ]                       [ PASSED VALIDATION ]
      mediation/rejected/<reason>/                    mediation/validated/
                                                                │
                                                                ▼
                      ╔═══════════════════════════════════════╗
                      ║     STAGE 2: RAW TABLE LOADER ENGINE  ║
                      ║         (task2_table_loader)          ║
                      ╚═══════════════════════════════════════╝
                                          │
                                src/table_loader.py
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
                    ▼                     ▼                     ▼
          [ 1. Idempotency Check ]   [ 2. Row Filtering ]   [ 3. Deduplication ]
          loaded_files_audit         Required Fields          In-Batch
          (Skips previously loaded)  (iccid, customernumber, (Keep latest pair)
                                     companyid checks)
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    ▼                                           ▼
      ┌───────────────────────────┐               ┌───────────────────────────┐
      │ Apache Doris: Target Table│               │Apache Doris: Reject Table │
      │  santhosh_actives_db.     │               │  santhosh_actives_db.     │
      │        raw_actives        │               │    raw_actives_rejected   │
      │   (Valid Ingested Rows)   │               │ (Bad Rows + Reject Reason)│
      └───────────────────────────┘               └───────────────────────────┘
                                          │
                                          ▼
                      ┌───────────────────────────────────────┐
                      │    Apache Doris: Audit State Table    │
                      │ santhosh_actives_db.loaded_files_audit│
                      │  (Tracks File Name, Loaded, Rejected) │
                      └───────────────────────────────────────┘



```


## 2. Project Folder Structure

```text
intern_assessment/
├── task1_file_validator/
│   ├── dags/
│   │   └── actives_ingestion_dag.py       # Airflow DAG definition and execution logic
│   └── src/
│       ├── __init__.py                    # Python package marker
│       ├── config.py                      # Constants, 46 column names, regex, MinIO paths
│       ├── file_validator.py              # Core 8-step validation & row recovery logic
│       └── storage.py                     # MinIO client connection, streaming, and routing helpers
├── task2_table_loader/
│   └── src/
│       ├── __init__.py                    # Python package marker
│       ├── config.py                      # Doris DB configs, table names, schema mapping
│       ├── storage.py                     # MinIO retrieval helper for validated files
│       └── table_loader.py                # Decompression, row validation, deduplication, batch DB writes
├── README.md                              # Documentation and architecture guide
└── .gitignore                             # Git ignore rules
```


## 3. Validation Rules & Execution Order

Every file is checked against 8 rules in this exact sequence (stopping at the first rule it fails):

1. **`pattern_mismatch`**: Verifies the file name follows `Region_Instance_(Active|Actives)_Sequence_Timestamp.csv(.gz)`.
2. **`zero_size`**: Rejects completely empty files (0 bytes).
3. **`header_only`**: Rejects files that have only a header and no data rows.
4. **`not_text_format`**: Rejects binary, corrupted, or unreadable files.
5. **`invalid_column_count` (Header)**: Rejects files where the header has fewer than 9 columns or does not match the 46 expected columns in exact order.
6. **`invalid_column_count` (Data Rows)**: Rejects files where data rows do not have the required 46 columns.
7. **`duplicate`**: Prevents reprocessing files that were already processed before.
8. **`sequence_invalid`**: Ensures sequence numbers for a region do not repeat or arrive out of sequence.

---


## 4. Key Design Decisions & Assumptions

### Handling Empty Values Between Semicolons (`;;`)
* In CSV feeds, consecutive delimiters like `;;` represent intentional null or empty values.
* Standard semicolon splitting (`line.split(";")`) is used so that every empty field is preserved as an empty string (`""`) rather than discarded.
* For example, in test files such as `HOL_01_Active_000000001_20260923000000.csv`, all 13,454 empty values across 636 rows are preserved, allowing all rows to maintain their structure.

### Flexible Trailing Column Tolerance & Auto-Padding (Handling 36 vs 46 Columns)
* In certain sample files (e.g., `SNG_01_Active_000000002_...csv`), the header contains 46 columns, but data rows have only 36 fields because trailing non-mandatory fields were truncated at generation.
* **Why the Minimum Threshold of 21 Columns Was Chosen:**
  - In the Actives 46-column feed specification, all mandatory operational and business keys are positioned within the first 21 columns:
    - **Column 1 (`CompanyID`):** Mandatory; rejected if null, empty, or zero.
    - **Column 12 (`CustomerNumber`):** Mandatory; rejected if null or empty.
    - **Column 21 (`ICCID`):** The primary unique key (`UNIQUE KEY(iccid)`) of the target table and mandatory; rejected if null or empty.
  - Columns 22 through 46 (such as `ExtraMSISDN_1` to `ExtraCreationDate_4`, `PlanName`, `Type`, `SIM_Vendor`, `EID`) represent secondary, trailing metadata and are not unique keys or mandatory rejection filters.
  - Any row with fewer than 21 fields is physically truncated before `ICCID`, rendering it impossible to identify or insert into Apache Doris. Therefore, rows with `< 21` fields are dropped as malformed.
* **Auto-Padding Logic (`extract_valid_rows_and_audit`):**
  - When a row contains between 21 and 45 fields, it possesses all critical business identifiers.
  - The engine dynamically calculates the missing field count (`46 - len(fields)`) and appends empty strings (`""`) to pad the row to the exact 46-column schema.
  - Rows exceeding 46 fields or falling below 21 fields are flagged as `ROW_FIELD_COUNT_OUT_OF_BOUNDS`, isolated, and recorded in the audit manifest.
* This approach prevents unnecessary data loss for valid SIM records while ensuring downstream database tables receive consistent, well-formed 46-column records.

### Non-Destructive Ingestion & Audit Manifest
* Source files in `crushftp/mediation/landing` are read in-place as byte streams without modifying or deleting raw source objects.
* When rows are sanitized or padded, an audit manifest JSON (`audit_manifest`) records `total_scanned_rows`, `valid_rows_retained`, `dropped_rows_count`, and sample error payloads for complete data observability.


## 5. How to Run the Pipeline

1. Open the **Airflow UI** (`http://34.130.209.7:32002`).
2. Locate the DAG: **`santhosh_actives_ingestion`**.
3. Ensure the DAG toggle is turned **ON**.
4. Click **Trigger DAG** (▶️).
5. View the task log under `validate_and_reject` and `load_raw_table` to see full execution details, files validated, loaded records, and rejected items with their specific reasons.


## 6. Verify the ingestion in Doris (via DBeaver / MySQL client):

   ```sql
   -- Check total loaded records (255,000 valid rows)
   SELECT count(*) AS total_raw_rows FROM santhosh_actives_db.raw_actives;

   -- Check rejected records (0 rows - all rows had valid iccid, customernumber, and companyid)
   SELECT count(*) AS total_rejected_rows FROM santhosh_actives_db.raw_actives_rejected;

   -- Check idempotency audit log (51 files processed)
   SELECT * FROM santhosh_actives_db.loaded_files_audit ORDER BY loaded_at DESC;
   ```
## 7. Verify Idempotency:

   - Click **Trigger DAG** (▶️) a second time in Airflow.
   - Inspect the logs for `load_raw_table`: the audit query finds all 51 files already logged (`Rows affected: 1`) and safely skips them, ensuring 0 duplicate rows are inserted.