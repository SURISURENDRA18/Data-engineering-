from datetime import datetime
import csv
import io

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from minio import Minio
import pymysql

SOURCE_BUCKET = "test"
SOURCE_FILE = "employees.csv"
DEST_SCHEMA = "database_SANTHOSHKUMAR"
DEST_TABLE = "employees"


def fetch_minio_client():
    conn = BaseHook.get_connection("minio_conn")
    return Minio(
        f"{conn.host}:{conn.port}",
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )


def fetch_doris_connection():
    conn = BaseHook.get_connection("doris_conn")
    return pymysql.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password or "",
        database=DEST_SCHEMA,
        autocommit=True,
    )


@dag(
    dag_id="santhosh_minio_doris_connectivity_test",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santhosh", "connectivity", "minio", "doris"],
)
def santhosh_connectivity_pipeline():

    @task
    def sync_minio_to_doris():
        # Step 1: pull file from MinIO
        client = fetch_minio_client()
        obj = client.get_object(SOURCE_BUCKET, SOURCE_FILE)
        text = obj.read().decode("utf-8")
        records = list(csv.DictReader(io.StringIO(text)))
        print(f"[MinIO] fetched {len(records)} record(s) from {SOURCE_BUCKET}/{SOURCE_FILE}")

        if not records:
            print("[Doris] nothing to insert, skipping load")
            return

        # Step 2: push into Doris
        db = fetch_doris_connection()
        try:
            with db.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {DEST_SCHEMA}.{DEST_TABLE} (
                        employee_id VARCHAR(15),
                        employee VARCHAR(50),
                        department VARCHAR(50),
                        salary DECIMAL(10,2)
                    ) UNIQUE KEY(employee_id) DISTRIBUTED BY HASH(employee_id) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                """)
                for r in records:
                    cursor.execute(
                        f"INSERT INTO {DEST_SCHEMA}.{DEST_TABLE} (employee_id, employee, department, salary) "
                        f"VALUES (%s, %s, %s, %s)",
                        (r["employee_id"], r["employee"], r["department"], r["salary"]),
                    )
            print(f"[Doris] inserted {len(records)} record(s) into {DEST_SCHEMA}.{DEST_TABLE}")
        finally:
            db.close()

    sync_minio_to_doris()


santhosh_connectivity_pipeline()