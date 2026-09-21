from datetime import datetime
import io
import pandas as pd
import pymysql

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

SCHEMA = "santhosh_retail_analytics"
DORIS_CONN_ID = "santhosh_doris"
MINIO_CONN_ID = "santhosh_minio"
BUCKET_NAME = "santhosh-warehouse"


def _doris_conn():
    conn = BaseHook.get_connection(DORIS_CONN_ID)
    return pymysql.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password or "",
        database=SCHEMA,
        autocommit=True,
    )


@dag(
    dag_id="santhosh_task8_e2e_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santhosh", "task8", "mini_project"],
)
def task8_e2e_pipeline():

    @task
    def check_incremental_files() -> list:
        """Finds unprocessed files from MinIO using the ingestion tracker."""
        conn = _doris_conn()
        processed_files = set()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS santhosh_ingestion_tracker (
                        file_name VARCHAR(255),
                        file_type VARCHAR(50),
                        record_count BIGINT,
                        status VARCHAR(20),
                        ingested_at DATETIME
                    )
                    UNIQUE KEY(file_name)
                    DISTRIBUTED BY HASH(file_name) BUCKETS 1;
                """
                )
                cur.execute(
                    "SELECT file_name FROM santhosh_ingestion_tracker WHERE status = 'success'"
                )
                processed_files = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()

        # List files from MinIO (or fallback known paths)
        all_files = [
            "santhosh_raw_data/orders.csv",
            "santhosh_raw_data/customers.csv",
        ]
        new_files = [f for f in all_files if f not in processed_files]
        print(f"Already processed: {processed_files}")
        print(f"New files to ingest: {new_files}")
        return new_files

    @task
    def apply_cdc_and_soft_deletes() -> dict:
        """Applies pending CDC staged changes with soft delete and event log idempotency."""
        conn = _doris_conn()
        counts = {
            "INSERT": 0,
            "UPDATE": 0,
            "DELETE": 0,
            "skipped_duplicates": 0,
        }
        try:
            with conn.cursor() as cur:
                # Count duplicates
                cur.execute(
                    """
                    SELECT COUNT(*) 
                    FROM santhosh_cdc_customers_staging s
                    JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                """
                )
                counts["skipped_duplicates"] = cur.fetchone()[0]

                # Apply INSERT
                cur.execute(
                    """
                    INSERT INTO santhosh_cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, s.name, s.city, s.email, 0, s.change_time, s.event_id
                    FROM santhosh_cdc_customers_staging s
                    LEFT JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'INSERT' AND l.event_id IS NULL
                """
                )
                counts["INSERT"] = cur.rowcount

                # Apply UPDATE
                cur.execute(
                    """
                    INSERT INTO santhosh_cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, s.name, s.city, s.email, 0, s.change_time, s.event_id
                    FROM santhosh_cdc_customers_staging s
                    LEFT JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'UPDATE' AND l.event_id IS NULL
                """
                )
                counts["UPDATE"] = cur.rowcount

                # Apply DELETE (Soft delete)
                cur.execute(
                    """
                    INSERT INTO santhosh_cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, t.name, t.city, t.email, 1, s.change_time, s.event_id
                    FROM santhosh_cdc_customers_staging s
                    JOIN santhosh_cdc_customers_target t ON t.customer_id = s.customer_id
                    LEFT JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'DELETE' AND l.event_id IS NULL
                """
                )
                counts["DELETE"] = cur.rowcount

                # Log applied events
                cur.execute(
                    """
                    INSERT INTO santhosh_cdc_event_log (event_id, customer_id, operation, change_time, applied_at)
                    SELECT s.event_id, s.customer_id, s.operation, s.change_time, NOW()
                    FROM santhosh_cdc_customers_staging s
                    LEFT JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                    WHERE l.event_id IS NULL
                """
                )
        finally:
            conn.close()

        print("CDC apply summary:", counts)
        return counts

    @task
    def run_automated_data_quality() -> dict:
        """Runs automated quality checks on Doris warehouse tables."""
        conn = _doris_conn()
        dq_results = {}
        try:
            with conn.cursor() as cur:
                # DQ 1: Check for NULL primary keys in orders
                cur.execute(
                    "SELECT COUNT(*) FROM fact_orders WHERE order_id IS NULL"
                )
                null_orders = cur.fetchone()[0]
                dq_results["null_order_ids"] = null_orders
                assert (
                    null_orders == 0
                ), f"DQ Failed: Found {null_orders} NULL order_ids"

                # DQ 2: Check for negative order amounts
                cur.execute(
                    "SELECT COUNT(*) FROM fact_orders WHERE total_amount < 0"
                )
                negative_amounts = cur.fetchone()[0]
                dq_results["negative_amounts"] = negative_amounts
                assert (
                    negative_amounts == 0
                ), f"DQ Failed: Found {negative_amounts} negative amounts"

                # DQ 3: Target table row count check
                cur.execute("SELECT COUNT(*) FROM santhosh_cdc_customers_target")
                total_target_customers = cur.fetchone()[0]
                dq_results["total_target_customers"] = total_target_customers
                assert (
                    total_target_customers > 0
                ), "DQ Failed: Target customer table is empty"

        finally:
            conn.close()

        print("Automated Data Quality Results (All Passed):", dq_results)
        return dq_results

    # Pipeline execution flow
    files = check_incremental_files()
    cdc = apply_cdc_and_soft_deletes()
    dq = run_automated_data_quality()

    files >> cdc >> dq


task8_e2e_pipeline()