# Actives Ingestion Assessment

Production-ready Airflow pipeline for validating Actives subscriber files in
MinIO and loading valid rows into Apache Doris.

## Pipeline

```text
MinIO landing -> validate_and_reject -> MinIO validated -> load_raw_table -> Doris
                         |
                         +-> MinIO rejected/<reason>/
```

The deployed DAG is `actives_ingestion_dag`. It is manually triggered and has
the strict dependency:

```text
validate_and_reject >> load_raw_table
```

## Runtime connections and locations

Airflow supplies credentials through `minio_default` and `doris_default`. No
passwords, tokens, or access keys belong in source control. Local execution may
use environment variables as a fallback.

MinIO:

- Bucket: `crushftp`
- Input: `mediation/landing/` (Shared permanent raw source, never deleted)
- Validated: `mediation/surendra/validated/`
- Rejected: `mediation/surendra/rejected/<reason>/`

Doris:

- Database: `surendra_db`
- Raw table: `raw_actives_daily`
- Rejected-row table: `raw_actives_daily_rejected`
- Tracking table: `actives_file_tracking`

The MySQL connection port is read from `doris_default`. The Stream Load HTTP
port is read from connection extra `http_port` or `httpPort`, with local
`DORIS_HTTP_PORT` as fallback.

## Task 1: file validation and routing

Files are processed in sorted object-key order and stop at the first failed
check:

| Order | Check | Reject reason |
| --- | --- | --- |
| 1 | Actives filename pattern | `pattern_mismatch` |
| 2 | File size is greater than zero | `zero_size` |
| 3 | Header has at least one data row | `header_only` |
| 4 | Content is readable text/valid gzip | `not_text_format` |
| 5 | Header has the expected 46 columns in order | `invalid_column_count` |
| 6 | Every data row has the header field count | `invalid_column_count` |
| 7 | File content is not a previously processed SHA-256 | `duplicate` |
| 8 | Region sequence is new and strictly increasing | `sequence_invalid` |

Passing files are copied to `mediation/surendra/validated/`. Failed files are copied to
`mediation/surendra/rejected/<reason>/`. File routing uses copy-only operations (landing files
remain permanently untouched), bounded concurrency, retries, and destination-exists idempotency.

## Task 2: row validation and loading

Each validated file is isolated from other files. Rows are checked in order:

1. Empty `ICCID` -> `NULL_OR_EMPTY_ICCID`
2. Empty `CustomerNumber` -> `NULL_OR_EMPTY_CUSTOMER_NUMBER`
3. Empty or zero `CompanyID` -> `NULL_OR_ZERO_COMPANY_ID`

Invalid rows are written to `raw_actives_daily_rejected`. Valid rows are
deduplicated within the file by `(CustomerNumber, ICCID)`, keeping the last
occurrence in source order. Loaded rows receive `file_name` and `loaded_at`.

The raw table uses `UNIQUE KEY(customernumber, iccid)`. The tracking table
stores file hash, filename, region, sequence, status, row counts, timestamp,
and error details. A successful file is not loaded again.

## Database schema

The SQL definitions are version-controlled in `sql/`:

- `create_raw_table.sql`
- `create_rejected_table.sql`
- `create_file_tracking_table.sql`

The pipeline verifies the database and tables at startup. It fails closed on
an incompatible schema and does not drop, rename, or replace existing tables.

## Repository layout

```text
dags/actives_ingestion_dag.py       Airflow orchestration
src/actives_ingestion/              Pipeline implementation
sql/                                Doris DDL
fixtures/                           Sample input files
tests/unit/                         Unit and contract tests
tests/integration/                  Pipeline and live-service tests
```

## Local verification

Use Python 3.11 or newer with the dependencies in `requirements.txt`:

```bash
python3 -m pytest tests/unit -q
python3 -m pytest tests/integration/test_pipeline.py -q
ruff check src/actives_ingestion dags/actives_ingestion_dag.py tests/unit
```

Validate the DAG directly with Airflow:

```bash
PYTHONPATH=src python3 - <<'PY'
from airflow.models import DagBag

bag = DagBag(dag_folder="dags/actives_ingestion_dag.py")
assert not bag.import_errors, bag.import_errors
dag = bag.dags["actives_ingestion_dag"]
assert dag.task_dict["load_raw_table"].upstream_task_ids == {"validate_and_reject"}
print("DAG import and dependency checks passed")
PY
```

Live verification should confirm Airflow task states/logs, MinIO routing, and
read-only Doris counts. Temporary live test files must be removed afterward.
Never include `.env`, credentials, tokens, virtual environments, cache
directories, or generated logs in a submission.
