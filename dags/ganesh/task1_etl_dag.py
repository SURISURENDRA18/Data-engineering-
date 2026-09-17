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

    check_input()

task1_etl()
