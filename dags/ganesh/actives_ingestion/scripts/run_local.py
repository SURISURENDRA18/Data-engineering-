"""Runs the Actives pipeline outside Airflow, against real MinIO/Doris,
using the exact same src/pipeline code the DAG calls. Useful for local
verification before/without a full Airflow deployment.

Reads credentials from environment variables (never hardcoded):
    MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    DORIS_HOST, DORIS_PORT, DORIS_USER, DORIS_PASSWORD

Usage:
    python scripts/run_local.py validate
    python scripts/run_local.py load
    python scripts/run_local.py all
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from src.clients.doris_client import get_doris_connection
from src.clients.minio_client import get_minio_client_from_conn
from src.config.settings import load_settings
from src.pipeline.file_validation_pipeline import run_validate_and_reject
from src.pipeline.raw_load_pipeline import run_raw_load


def main() -> None:
    stage = sys.argv[1] if len(sys.argv) > 1 else "all"
    settings = load_settings()

    s3_client = get_minio_client_from_conn(settings.minio_conn_id)
    conn = get_doris_connection(settings.doris_conn_id, database=settings.doris_database)
    try:
        if stage in ("validate", "all"):
            run_validate_and_reject(s3_client, conn, settings)
        if stage in ("load", "all"):
            run_raw_load(s3_client, conn, settings)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
