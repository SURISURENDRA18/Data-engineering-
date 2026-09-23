# Task 1: File Validator and Routing Pipeline

This project is the first stage of our data ingestion pipeline. It automatically reads incoming daily `Actives` files from the landing area, checks them against validation rules, and routes them to the correct MinIO folders.

---

## 1. Project Folder Structure

Inside `task1_file_validator/`, the files are organized as follows:

* **`dags/actives_ingestion_dag.py`**[cite: 8]:
  The Airflow DAG definition (`santhosh_actives_ingestion`)[cite: 5, 8]. It scans the landing folder, filters for Active feeds, runs all validation checks, moves files to their target folders, and generates an audit log[cite: 6].
* **`src/file_validator.py`**[cite: 1]:
  The core validation engine[cite: 6]. It contains:
  - `validate_file()`: Runs the 8 sequential file checks (file name pattern, size, header, text format, column counts, duplicates, and sequence order)[cite: 1, 6].
  - `extract_valid_rows_and_audit()`: Scans data row by row, preserves valid rows, isolates malformed rows, and prepares audit metadata[cite: 1].
* **`src/config.py`**[cite: 1]:
  Configuration file holding constants such as delimiter (`;`), expected 46 column names in order, file regex patterns, and MinIO storage paths[cite: 1, 6].

---

## 2. Validation Rules & Execution Order

Every file is checked against 8 rules in this exact sequence (stopping at the first rule it fails)[cite: 6]:

1. **`pattern_mismatch`**: Verifies the file name follows `Region_Instance_(Active|Actives)_Sequence_Timestamp.csv(.gz)`[cite: 6].
2. **`zero_size`**: Rejects completely empty files (0 bytes)[cite: 6].
3. **`header_only`**: Rejects files that have only a header and no data rows[cite: 6].
4. **`not_text_format`**: Rejects binary, corrupted, or unreadable files[cite: 6].
5. **`invalid_column_count` (Header)**: Rejects files where the header has fewer than 9 columns or does not match the 46 expected columns in exact order[cite: 6].
6. **`invalid_column_count` (Data Rows)**: Rejects files where data rows do not have the required 46 columns[cite: 6].
7. **`duplicate`**: Prevents reprocessing files that were already processed before[cite: 6].
8. **`sequence_invalid`**: Ensures sequence numbers for a region do not repeat or arrive out of sequence[cite: 6].

---

## 3. Key Design Decisions & Assumptions

### Handling Empty Values Between Semicolons (`;;`)
* In our CSV data, consecutive delimiters like `;;` represent intentional empty/null values[cite: 6].
* We use standard semicolon splitting (`line.split(";")`)[cite: 1]. This ensures empty fields are counted as valid empty strings (`""`) rather than dropped[cite: 1].
* For example, the test file `HOL_01_Active_000000001_20260923000000.csv` has **13,454 empty values**, but because each empty field is counted, all 636 rows have all **46 columns** intact and pass validation successfully.

### Why Certain Files Are Rejected as `invalid_column_count`
* In bad sample files like `SNG_01_Active_000000002_20260922003000.csv`, data rows have only 35 delimiters (36 columns) while the header has 46 columns[cite: 6].
* Even after counting every empty field, the last 10 columns are physically missing from each data row. Because 36 does not equal 46, this file is correctly routed to `rejected/invalid_column_count/`[cite: 6].

### Row Resilience & Error Isolation
* In production data pipelines, dropping an entire batch of thousands of rows because of a few broken rows causes business data loss[cite: 6].
* To handle this, we implemented row resilience (`extract_valid_rows_and_audit`)[cite: 1]:
  - Rows with the correct 46 columns are preserved[cite: 1, 6].
  - Rows with incorrect column counts are isolated and tracked[cite: 1].
  - A detailed manifest JSON is created recording total rows, retained rows, dropped rows, and row error samples[cite: 1].
  - An `audit_summary.csv` file is uploaded to `crushftp/mediation/santhosh/audit/` so the team can monitor data quality at any time.

### Non-Destructive Ingestion
* Source files in `crushftp/mediation/landing` are read as byte streams in place[cite: 6].
* We never delete or modify the original files in the landing bucket[cite: 6].
* Files are copied to `validated/` or routed into `rejected/<reason>/` based on the validation result[cite: 6].

---

## 4. How to Run the Pipeline

1. Open the **Airflow UI** (`http://34.130.209.7:32002`)[cite: 5].
2. Locate the DAG: **`santhosh_actives_ingestion`**[cite: 5].
3. Ensure the DAG toggle is turned **ON**[cite: 5].
4. Click **Trigger DAG** (▶️)[cite: 5].
5. View the task log under `validate_and_route_files` to see full execution details, files validated, and files rejected with their specific reasons[cite: 5, 6].
