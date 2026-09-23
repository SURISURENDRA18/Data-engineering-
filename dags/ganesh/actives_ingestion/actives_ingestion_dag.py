"""Actives feed ingestion DAG (Data Engineering Intern Assessment).

SOURCE: MinIO crushftp/mediation/landing/ (shared -- every candidate's
    pipeline in this environment reads from the same source)
STAGE 1 (validate_and_reject): validate each Active-named file against the
    8 file-level checks in src/pipeline/file_validation_pipeline.py; copy
    (never delete the original) to mediation/ganesh/validated/ or
    mediation/ganesh/rejected/<reason>/. Files not named "Active"/"Actives"
    are ignored entirely -- they belong to other feeds/candidates sharing
    this landing zone.
STAGE 2 (load_raw_table): load mediation/ganesh/validated/ files into
    Doris raw_actives_daily via src/pipeline/raw_load_pipeline.py, with
    row-level rejection, dedup, and idempotent per-file loading.

This file is intentionally thin -- orchestration only. All filename/CSV/
SQL/validation/dedup logic lives in src/. See README.md for the full
pipeline contract, table data dictionary, and troubleshooting runbook.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# This package lives at dags/ganesh/actives_ingestion/ -- add its own
# directory to sys.path so `from src....` imports resolve, matching the
# pattern already used by dags/others/feed_simulator/feed_simulator_dag.py
# in this same repo.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

from src.clients.doris_client import get_doris_connection  # noqa: E402
from src.clients.minio_client import get_minio_client_from_conn  # noqa: E402
from src.config.settings import load_settings  # noqa: E402
from src.pipeline.file_validation_pipeline import run_validate_and_reject  # noqa: E402
from src.pipeline.raw_load_pipeline import run_raw_load  # noqa: E402

DAG_DESCRIPTION = (
    "Ingests the daily Actives SIM feed from the shared MinIO landing zone "
    "(crushftp/mediation/landing/). Validates Active-named files against "
    "filename, schema, format, duplicate, and sequence rules; copies (never "
    "deletes the original -- landing/ is shared with other candidates) "
    "invalid files to reason-specific mediation/ganesh/rejected/<reason>/ "
    "paths; and loads validated records into the Doris raw_actives_daily "
    "table with row-level validation, deduplication, metadata, and "
    "idempotent per-file processing."
)

VALIDATE_DOC = """\
Only looks at filenames containing "Active" -- everything else in the
shared landing/ zone (other feeds, other candidates) is left untouched.
For each in-scope file, validates against, in order:
1. filename pattern
2. file size > 0
3. has at least one data row below the header
4. text readability (not binary/corrupted)
5. 46-column header schema, exact order
6. no row has more fields than the header (a missing trailing field is
   padded as empty, not treated as a rejection)
7. duplicate detection (content checksum)
8. per-region sequence validation

Valid files are **copied** (never removed from landing/) to
`mediation/ganesh/validated/`. Invalid files are copied to
`mediation/ganesh/rejected/<reason>/`. Idempotency is filename-based
(already-routed files are skipped), not based on the file's absence from
landing/. One bad file cannot stop the batch -- check this task's logs
for a per-file decision line (FILE_VALIDATED / FILE_REJECTED) and a run
summary block.
"""

LOAD_DOC = """\
Reads files from `mediation/ganesh/validated/`, applies row-level
rejection rules (NULL_OR_EMPTY_ICCID, NULL_OR_EMPTY_CUSTOMER_NUMBER,
NULL_OR_ZERO_COMPANY_ID), deduplicates on iccid (keep last occurrence),
and loads the result into Doris `raw_actives_daily`. Rejected rows go to
`raw_actives_daily_rejected`. Files already loaded (by content checksum)
are skipped. One failing file cannot stop the batch -- check this task's
logs for per-file row counts and a run summary block.
"""


@dag(
    dag_id="ganesh_actives_ingestion",
    description=DAG_DESCRIPTION,
    schedule=None,  # no cadence specified by the assessment; manual/external trigger, see README
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["de102", "actives", "ingestion", "minio", "doris"],
)
def ganesh_actives_ingestion():
    @task(task_id="validate_and_reject", doc_md=VALIDATE_DOC)
    def validate_and_reject_task() -> dict:
        settings = load_settings()
        s3_client = get_minio_client_from_conn(settings.minio_conn_id)
        conn = get_doris_connection(settings.doris_conn_id, database=settings.doris_database)
        try:
            return run_validate_and_reject(s3_client, conn, settings)
        finally:
            conn.close()

    @task(task_id="load_raw_table", doc_md=LOAD_DOC)
    def load_raw_table_task(_validation_summary: dict) -> dict:
        settings = load_settings()
        s3_client = get_minio_client_from_conn(settings.minio_conn_id)
        conn = get_doris_connection(settings.doris_conn_id, database=settings.doris_database)
        try:
            return run_raw_load(s3_client, conn, settings)
        finally:
            conn.close()

    load_raw_table_task(validate_and_reject_task())


ganesh_actives_ingestion()
