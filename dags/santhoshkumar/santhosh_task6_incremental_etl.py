import csv
import io
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from minio import Minio

BUCKET = "test"
PREFIX = "santhosh_raw_data/"
SCHEMA = "santhosh_retail_analytics"
DORIS_CONN_ID = "santhosh_doris"


def _minio_client():
    try:
        conn = BaseHook.get_connection("santhosh_minio")
    except Exception:
        conn = BaseHook.get_connection("minio_default")

    return Minio(
        f"{conn.host}:{conn.port}",
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )


def _doris_conn():
    import pymysql

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
    dag_id="santhosh_task6_incremental_etl",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santhosh", "task6", "incremental"],
)
def task6_incremental_etl():

    @task
    def find_new_files() -> list[str]:
        client = _minio_client()
        all_files = [
            obj.object_name
            for obj in client.list_objects(BUCKET, prefix=PREFIX, recursive=True)
            if obj.object_name.endswith(".csv")
        ]

        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_name FROM santhosh_ingestion_tracker WHERE status = 'success'"
                )
                already_done = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()

        new_files = [f for f in all_files if f not in already_done]
        print(f"All files detected: {all_files}")
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
                if "customer" in file_key.lower():
                    for row in rows:
                        cur.execute(
                            """
                            INSERT INTO dim_customer (customer_id, customer_name, city, email, created_date)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                row["customer_id"],
                                row["customer_name"],
                                row["city"],
                                row["email"],
                                row["created_date"],
                            ),
                        )
                elif "order" in file_key.lower():
                    for row in rows:
                        qty = int(row["quantity"])
                        price = float(row["unit_price"])
                        cur.execute(
                            """
                            INSERT INTO fact_orders (order_id, customer_key, order_date, quantity, unit_price, total_amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                row["order_id"],
                                int(row["customer_id"]),
                                row["order_date"],
                                qty,
                                price,
                                qty * price,
                            ),
                        )

                cur.execute(
                    """
                    INSERT INTO santhosh_ingestion_tracker (file_name, status, processed_at, record_count, error_message)
                    VALUES (%s, 'success', NOW(), %s, NULL)
                    """,
                    (file_key, len(rows)),
                )

            print(f"Processed {file_key}: {len(rows)} rows -> status: success")

        except Exception as exc:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO santhosh_ingestion_tracker (file_name, status, processed_at, record_count, error_message)
                    VALUES (%s, 'failed', NOW(), 0, %s)
                    """,
                    (file_key, str(exc)[:900]),
                )
            raise
        finally:
            conn.close()

    new_files = find_new_files()
    process_file.expand(file_key=new_files)


task6_incremental_etl()