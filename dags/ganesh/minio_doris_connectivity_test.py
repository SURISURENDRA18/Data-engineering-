from datetime import datetime
import csv
import io

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from minio import Minio
from airflow.providers.mysql.hooks.mysql import MySqlHook

BUCKET = "test"
OBJECT_KEY = "sample.csv"
TARGET_TABLE = "database_ganesh.connectivity_test"


@dag(
    dag_id="ganesh_minio_doris_connectivity_test",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "ganesh", "minio", "doris"],
)
def minio_doris_connectivity_test():

    @task
    def read_from_minio() -> list[dict]:
        conn = BaseHook.get_connection("minio_conn")
        client = Minio(
            f"{conn.host}:{conn.port}",
            access_key=conn.login,
            secret_key=conn.password,
            secure=False,
        )
        response = client.get_object(BUCKET, OBJECT_KEY)
        content = response.read().decode("utf-8")
        rows = list(csv.DictReader(io.StringIO(content)))
        print(f"Read {len(rows)} row(s) from minio://{BUCKET}/{OBJECT_KEY}")
        return rows

    @task
    def load_to_doris(rows: list[dict]):
        if not rows:
            print("No rows to load — check the MinIO read step")
            return
        hook = MySqlHook(mysql_conn_id="doris_conn")
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                msisdn VARCHAR(15),
                plan_code VARCHAR(20),
                signup_date DATE
            ) UNIQUE KEY(msisdn) DISTRIBUTED BY HASH(msisdn) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """)
        for row in rows:
            hook.run(
                f"INSERT INTO {TARGET_TABLE} (msisdn, plan_code, signup_date) VALUES (%s, %s, %s)",
                parameters=(row["msisdn"], row["plan_code"], row["signup_date"]),
            )
        print(f"Loaded {len(rows)} row(s) into {TARGET_TABLE}")

    load_to_doris(read_from_minio())


minio_doris_connectivity_test()