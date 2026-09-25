"""Airflow DAG for Actives Ingestion Pipeline.

Pipeline Flow:
validate_and_reject >> load_raw_table
"""

import os
import sys
from datetime import datetime, timedelta

# Ensure src package is importable
SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from actives_ingestion.file_handler import FileHandler
from actives_ingestion.raw_loader import RawLoader
from actives_ingestion.utils import get_logger

logger = get_logger("actives_ingestion_dag")


def task_validate_and_reject(**context):
    """Task 1: Discovers landing files, runs Checks 1-8, and routes to validated/rejected folders."""
    logger.info("Executing Task 1: validate_and_reject")
    handler = FileHandler()
    conf = (context.get("dag_run") and getattr(context["dag_run"], "conf", None)) or {}
    feed_filter = conf.get("feed_filter") or os.getenv("FEED_FILTER")
    filter_fn = (lambda f: feed_filter in f) if feed_filter else None
    summary = handler.process_landing_files(file_filter=filter_fn)
    summary.log_summary(logger.info)
    return {
        "files_discovered": summary.files_discovered,
        "files_validated": summary.files_validated,
        "files_rejected": summary.files_rejected,
        "files_failed": summary.files_failed,
    }


def task_load_raw_table(**context):
    """Task 2: Ingests validated files into Doris raw tables with row validation, deduplication, and tracking."""
    logger.info("Executing Task 2: load_raw_table")
    loader = RawLoader()
    conf = (context.get("dag_run") and getattr(context["dag_run"], "conf", None)) or {}
    feed_filter = conf.get("feed_filter") or os.getenv("FEED_FILTER")
    filter_fn = (lambda f: feed_filter in f) if feed_filter else None
    summary = loader.load_all_validated_files(file_filter=filter_fn)
    summary.log_summary(logger.info)
    return {
        "files_loaded": summary.files_loaded,
        "total_rows_loaded": summary.total_rows_loaded,
        "total_rows_rejected": summary.total_rows_rejected,
        "files_failed": summary.files_failed,
    }


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

try:
    from airflow import DAG
    try:
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="surendra_actives_ingestion",
        default_args=default_args,
        description="Ingestion and validation pipeline for active telecom subscribers",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["surendra", "actives", "mediation", "telecom", "doris"],
    ) as dag:

        validate_and_reject = PythonOperator(
            task_id="validate_and_reject",
            python_callable=task_validate_and_reject,
            execution_timeout=timedelta(minutes=30),
        )

        load_raw_table = PythonOperator(
            task_id="load_raw_table",
            python_callable=task_load_raw_table,
            trigger_rule="all_success",
            execution_timeout=timedelta(minutes=30),
        )

        validate_and_reject >> load_raw_table


except ImportError:
    dag = None
    logger.info("Apache Airflow is not installed in this environment; DAG object defined conditionally.")


if __name__ == "__main__":
    task_validate_and_reject()
    task_load_raw_table()
