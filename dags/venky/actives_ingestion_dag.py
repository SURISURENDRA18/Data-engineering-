from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from venky.file_handler import process_files
from venky.loader import load_validated_files


with DAG(
    dag_id="venky_actives_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "venky",
        "actives",
        "ingestion",
    ],
    description=(
        "Validate Actives files in MinIO "
        "and load validated files into Doris."
    ),
) as dag:

    @task(
        task_id="validate_and_reject"
    )
    def validate_and_reject():
        process_files()

    @task(
        task_id="load_raw_table"
    )
    def load_raw_table():
        load_validated_files()

    validation = validate_and_reject()

    loading = load_raw_table()

    validation >> loading