import csv
import io
from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from minio import Minio

BUCKET = "de102-ganesh"

@dag(
    dag_id="de102_ganesh_task1_etl",
    schedule=None,
    start_date=datetime(2026, 9, 17),
    catchup=False,
    tags=["de102", "task1"],
)
def task1_etl():

    @task
    def check_input():
        conn = BaseHook.get_connection("minio_default")
        client = Minio(
            f"{conn.host}:{conn.port}",
            access_key=conn.login,
            secret_key=conn.password,
            secure=False,
        )
        objects = client.list_objects(BUCKET, prefix="raw/", recursive=True)
        found = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]
        print(f"Found files: {found}")
        if not found:
            raise ValueError("No input CSV files found under raw/")
        return found

    @task
    def load_staging(files: list[str]):
        import pymysql

        minio_conn = BaseHook.get_connection("minio_default")
        client = Minio(
            f"{minio_conn.host}:{minio_conn.port}",
            access_key=minio_conn.login,
            secret_key=minio_conn.password,
            secure=False,
        )

        doris_conn = BaseHook.get_connection("doris_default")
        conn = pymysql.connect(
            host=doris_conn.host,
            port=doris_conn.port,
            user=doris_conn.login,
            password=doris_conn.password or "",
            database="database_ganesh",
            autocommit=True,
        )
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
                                (row["customer_id"], row["customer_name"], row["city"],
                                 row["email"], row["created_date"], key),
                            )
                    else:
                        for row in rows:
                            cur.execute(
                                "INSERT INTO stg_orders "
                                "(order_id, customer_id, order_date, quantity, unit_price, source_file) "
                                "VALUES (%s, %s, %s, %s, %s, %s)",
                                (row["order_id"], row["customer_id"], row["order_date"],
                                 row["quantity"], row["unit_price"], key),
                            )
                print(f"Loaded {len(files)} file(s) into staging.")
        finally:
            conn.close()
        return files

    load_staging(check_input())

task1_etl()