# Task 1: File Handler and Validator

Task 1 is the first stage of the ingestion pipeline. It scans incoming daily Actives files (active SIM records for a region) from the MinIO landing location (`crushftp/mediation/landing`). It checks each file against a strict list of 8 sequential validation rules. If a file passes every rule, it routes it to `validated/`; if it fails any rule, it moves it to `rejected/<reason>/` using the matching reason name as the folder name.

---

## 1. Architecture Overview

```text
               AIRFLOW (Orchestration Only)
                           |
               actives_ingestion_dag.py
                           |
             +-------------+-------------+
             |                           |
        File Stream                 Config & Schema
    (Non-destructive)               (src/config.py)
             |                           |
             +-------------+-------------+
                           |
                 src/file_validator.py
             (8-Rule Validation Engine)
                           |
        +------------------+------------------+
        |                                     |
   [PASS ALL]                            [FAIL ANY]
        |                                     |
MinIO: validated/                     MinIO: rejected/<reason>/
(HOL_01_Active_...)                   (invalid_column_count,
                                       pattern_mismatch, etc.)
                                              |
                                      Audit & Manifest
                                      (audit_summary.csv)


## 2. Project Folder Structure

task1_file_validator/
├── dags/
│   └── actives_ingestion_dag.py     # Airflow DAG definition and execution logic
├── src/
│   ├── config.py                    # Constants, 46 column names, regex, MinIO paths
│   └── file_validator.py            # Core 8-step validation & row recovery logic
└── README.md                        # Documentation and architecture guide

Inside `task1_file_validator/`, the files are organized as follows:

* **`dags/actives_ingestion_dag.py`**:
  The Airflow DAG definition (`santhosh_actives_ingestion`). It scans the landing folder, filters for Active feeds, runs all validation checks, moves files to their target folders, and generates an audit log.
* **`src/file_validator.py`**:
  The core validation engine. It contains:
  - `validate_file()`: Runs the 8 sequential file checks (file name pattern, size, header, text format, column counts, duplicates, and sequence order).
  - `extract_valid_rows_and_audit()`: Scans data row by row, preserves valid rows, isolates malformed rows, and prepares audit metadata.
* **`src/config.py`**:
  Configuration file holding constants such as delimiter (`;`), expected 46 column names in order, file regex patterns, and MinIO storage paths.

---

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
* In our CSV data, consecutive delimiters like `;;` represent intentional empty/null values.
* We use standard semicolon splitting (`line.split(";")`). This ensures empty fields are counted as valid empty strings (`""`) rather than dropped.
* For example, the test file `HOL_01_Active_000000001_20260923000000.csv` has **13,454 empty values**, but because each empty field is counted, all 636 rows have all **46 columns** intact and pass validation successfully.

### Why Certain Files Are Rejected as `invalid_column_count`
* In bad sample files like `SNG_01_Active_000000002_20260922003000.csv`, data rows have only 35 delimiters (36 columns) while the header has 46 columns.
* Even after counting every empty field, the last 10 columns are physically missing from each data row. Because 36 does not equal 46, this file is correctly routed to `rejected/invalid_column_count/`.

### Row Resilience & Error Isolation
* In production data pipelines, dropping an entire batch of thousands of rows because of a few broken rows causes business data loss.
* To handle this, we implemented row resilience (`extract_valid_rows_and_audit`):
  - Rows with the correct 46 columns are preserved.
  - Rows with incorrect column counts are isolated and tracked.
  - A detailed manifest JSON is created recording total rows, retained rows, dropped rows, and row error samples.
  - An `audit_summary.csv` file is uploaded to `crushftp/mediation/santhosh/audit/` so the team can monitor data quality at any time.

### Non-Destructive Ingestion
* Source files in `crushftp/mediation/landing` are read as byte streams in place.
* We never delete or modify the original files in the landing bucket.
* Files are copied to `validated/` or routed into `rejected/<reason>/` based on the validation result.

---

## 5. How to Run the Pipeline

1. Open the **Airflow UI** (`http://34.130.209.7:32002`).
2. Locate the DAG: **`santhosh_actives_ingestion`**.
3. Ensure the DAG toggle is turned **ON**.
4. Click **Trigger DAG** (▶️).
5. View the task log under `validate_and_route_files` to see full execution details, files validated, and files rejected with their specific reasons.
