# Actives Ingestion Assessment

This repository contains the deployed Airflow assessment pipeline
`actives_ingestion_dag`.

## Pipeline

```text
validate_and_reject -> load_raw_table
```

The DAG is manually triggered and uses the Airflow connections
`minio_default` and `doris_default`. No credentials are stored in source code.

## Storage and database

- MinIO bucket: `crushftp`
- Landing prefix: `mediation/landing/`
- Validated prefix: `mediation/validated/`
- Rejected prefix: `mediation/rejected/<reason>/`
- Doris database: `actives_db`
- Raw table: `raw_actives_daily`
- Rejected-row table: `raw_actives_daily_rejected`
- File tracking table: `actives_file_tracking`

Task 1 routes each landing object to exactly one destination after the ordered
filename, size, content, header, row-shape, duplicate, and sequence checks.
Task 2 validates rows, keeps the last occurrence of each
`(CustomerNumber, ICCID)` pair within a file, writes rejected rows to Doris,
loads valid rows, and records file-level status and counts.

## Operational assumptions

- The Airflow worker identity has list/read permissions on the landing,
  validated, and rejected MinIO prefixes, plus copy/delete permissions for
  routing.
- `doris_default` uses the Doris MySQL connection port. Its extra settings may
  provide `http_port` or `httpPort` for Stream Load; otherwise the configured
  `DORIS_HTTP_PORT` fallback is used locally.
- The raw table contract is `UNIQUE KEY(customernumber, iccid)`.
- Files are processed in sorted object-key order. A successful file-tracking
  record prevents the same file content from being loaded again.
- The DAG has no schedule and must be triggered manually.

## Verification

Run locally:

```bash
python3 -m pytest tests/unit -q
python3 -m pytest tests/integration/test_pipeline.py -q
ruff check src/actives_ingestion dags/actives_ingestion_dag.py tests/unit
```

The live deployment should be verified through Airflow task status/logs and
read-only MinIO/Doris checks. Test data must be cleaned up after a live test.
