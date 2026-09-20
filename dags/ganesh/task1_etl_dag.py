"""
Task 1 — ETL: File to Database
Flow: start -> check_input -> load_staging -> transform -> load_target -> validate -> end

Source files live in MinIO bucket `de102-ganesh` under raw/customers/ and
raw/orders/. This DAG loads whatever CSVs are found there into staging,
transforms + upserts into the curated customers/orders tables (calculating
total_amount), then validates the final load is non-empty.

Row-level data quality (nulls, dup ids, bad dates, negative qty/price,
orphan FKs) is handled by the separate `ganesh_task3_data_quality` DAG,
which reads the same staging tables. This DAG focuses purely on the
happy-path ETL flow from the assignment's Task 1.
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
    dag_id="de102_ganesh_task1_etl",
    schedule=None,
    start_date=datetime(2026, 9, 17),
    catchup=False,
    tags=["de102", "task1", "etl"],
)
def task1_etl():

    @task
    def check_input() -> list[str]:
        client = _minio_client()
        objects = client.list_objects(BUCKET, prefix="raw/", recursive=True)
        found = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]
        print(f"Found files: {found}")
        if not found:
            raise ValueError("No input CSV files found under raw/")
        return found

    @task
    def load_staging(files: list[str]) -> list[str]:
        client = _minio_client()
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE stg_customers")
                cur.execute("TRUNCATE TABLE stg_orders")

                for key in files:
                    response = client.get_object(BUCKET, key)
                    content = response.read().decode("utf-8")
                    rows = list(csv.DictReader(io.StringIO(content)))

                    if "customers" in key:
                        for row in rows:
                            cur.execute(
                                "INSERT INTO stg_customers "
                                "(customer_id, customer_name, city, email, created_date, source_file) "
                                "VALUES (%s, %s, %s, %s, %s, %s)",
                                (row["customer_id"] or None, row["customer_name"], row["city"],
                                 row["email"], row["created_date"], key),
                            )
                    else:
                        for row in rows:
                            cur.execute(
                                "INSERT INTO stg_orders "
                                "(order_id, customer_id, order_date, quantity, unit_price, source_file) "
                                "VALUES (%s, %s, %s, %s, %s, %s)",
                                (row["order_id"] or None, row["customer_id"], row["order_date"],
                                 row["quantity"], row["unit_price"], key),
                            )
            print(f"Loaded {len(files)} file(s) into staging.")
        finally:
            conn.close()
        return files

    @task
    def transform_and_load_target(files: list[str]) -> None:
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO customers (customer_id, customer_name, city, email, created_date)
                    SELECT customer_id, customer_name, city, email,
                           STR_TO_DATE(created_date, '%Y-%m-%d')
                    FROM stg_customers
                    WHERE customer_id IS NOT NULL
                      AND customer_id NOT IN (
                          SELECT customer_id FROM stg_customers
                          GROUP BY customer_id HAVING COUNT(*) > 1
                      )
                      AND (created_date IS NULL OR STR_TO_DATE(created_date, '%Y-%m-%d') IS NOT NULL)
                """)
                cur.execute("""
                    INSERT INTO orders (order_id, customer_id, order_date, quantity, unit_price, total_amount)
                    SELECT CAST(o.order_id AS SIGNED), o.customer_id,
                           STR_TO_DATE(o.order_date, '%Y-%m-%d'),
                           o.quantity, o.unit_price,
                           CAST(o.quantity AS DECIMAL(12,2)) * o.unit_price
                    FROM stg_orders o
                    WHERE o.order_id IS NOT NULL AND o.order_id REGEXP '^[0-9]+$'
                      AND o.order_id NOT IN (
                          SELECT order_id FROM stg_orders
                          GROUP BY order_id HAVING COUNT(*) > 1
                      )
                      AND o.quantity IS NOT NULL AND o.quantity > 0
                      AND o.unit_price IS NOT NULL AND o.unit_price >= 0
                      AND (o.order_date IS NULL OR STR_TO_DATE(o.order_date, '%Y-%m-%d') IS NOT NULL)
                      AND (o.customer_id IS NULL OR EXISTS (
                          SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id
                      ))
                """)
            print("Transform + load into curated customers/orders complete.")
        finally:
            conn.close()

    @task
    def validate() -> None:
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM customers")
                customers_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM orders")
                orders_count = cur.fetchone()[0]
            print(f"customers={customers_count}, orders={orders_count}")
            if customers_count == 0 or orders_count == 0:
                raise ValueError(
                    f"Validation failed: customers={customers_count}, orders={orders_count} "
                    "-- final load must contain records."
                )
        finally:
            conn.close()

    files = check_input()
    loaded = load_staging(files)
    transformed = transform_and_load_target(loaded)
    transformed >> validate()


task1_etl()
