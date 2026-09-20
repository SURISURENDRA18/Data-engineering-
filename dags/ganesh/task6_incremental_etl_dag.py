"""
Task 6 — Incremental ETL

Processes only files under raw/customers/ and raw/orders/ that have not
already succeeded, tracked in ingestion_tracker (UNIQUE KEY(file_name), so
re-inserting the same file_name is an upsert, not a duplicate row).

Safety / re-run guarantees:
  - Before loading a file, check ingestion_tracker for a 'success' row with
    that file_name. If found, skip it.
  - A file is only marked 'success' AFTER its rows are committed into the
    curated tables. If the task raises partway through, no tracker row (or
    a 'failed' row) is written, so the next run retries that file instead
    of skipping it.
  - Re-running this DAG after a successful run is a no-op (all files
    already marked success get skipped) -- this is the "safe to re-run"
    requirement and the "run more than once" demonstration.
"""
import csv
import io
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from minio import Minio

BUCKET = "de102-ganesh"
SCHEMA = "database_ganesh"


def _minio_client():
    conn = BaseHook.get_connection("minio_default")
    return Minio(
        f"{conn.host}:{conn.port}",
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )


def _doris_conn():
    import pymysql
    conn = BaseHook.get_connection("doris_default")
    return pymysql.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password or "",
        database=SCHEMA,
        autocommit=True,
    )


@dag(
    dag_id="ganesh_task6_incremental_etl",
    schedule=None,
    start_date=datetime(2026, 9, 18),
    catchup=False,
    tags=["de102", "task6", "incremental"],
)
def task6_incremental_etl():

    @task
    def find_new_files() -> list[str]:
        client = _minio_client()
        all_files = [
            obj.object_name
            for obj in client.list_objects(BUCKET, prefix="raw/", recursive=True)
            if obj.object_name.endswith(".csv")
        ]

        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT file_name FROM ingestion_tracker WHERE status = 'success'")
                already_done = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()

        new_files = [f for f in all_files if f not in already_done]
        print(f"All files: {all_files}")
        print(f"Already successful: {sorted(already_done)}")
        print(f"New files to process: {new_files}")
        return new_files

    @task
    def process_file(file_key: str) -> None:
        conn = _doris_conn()
        try:
            client = _minio_client()
            response = client.get_object(BUCKET, file_key)
            content = response.read().decode("utf-8")
            rows = list(csv.DictReader(io.StringIO(content)))

            with conn.cursor() as cur:
                if "customers" in file_key:
                    for row in rows:
                        cur.execute(
                            "INSERT INTO customers (customer_id, customer_name, city, email, created_date) "
                            "VALUES (%s, %s, %s, %s, %s)",
                            (row["customer_id"], row["customer_name"], row["city"],
                             row["email"], row["created_date"]),
                        )
                else:
                    for row in rows:
                        qty = int(row["quantity"])
                        price = float(row["unit_price"])
                        cur.execute(
                            "INSERT INTO orders (order_id, customer_id, order_date, quantity, unit_price, total_amount) "
                            "VALUES (%s, %s, %s, %s, %s, %s)",
                            (row["order_id"], row["customer_id"], row["order_date"],
                             qty, price, qty * price),
                        )

                # Mark success only after rows are committed (autocommit=True
                # above means each INSERT is already durable at this point).
                cur.execute(
                    "INSERT INTO ingestion_tracker (file_name, status, processed_at, record_count, error_message) "
                    "VALUES (%s, 'success', NOW(), %s, NULL)",
                    (file_key, len(rows)),
                )
            print(f"Processed {file_key}: {len(rows)} record(s) -> marked success.")
        except Exception as exc:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ingestion_tracker (file_name, status, processed_at, record_count, error_message) "
                    "VALUES (%s, 'failed', NOW(), 0, %s)",
                    (file_key, str(exc)[:900]),
                )
            raise
        finally:
            conn.close()

    new_files = find_new_files()
    process_file.expand(file_key=new_files)


task6_incremental_etl()
