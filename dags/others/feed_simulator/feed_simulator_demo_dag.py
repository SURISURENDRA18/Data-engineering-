from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
import os
import sys


# =========================================================
# PROJECT PATHS
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = BASE_DIR

GENERATOR_DIR = os.path.join(
    PROJECT_ROOT,
    "generators",
)

sys.path.append(PROJECT_ROOT)
sys.path.append(GENERATOR_DIR)


# =========================================================
# IMPORTS
# =========================================================

from data_feed_generator import (
    generate_data_feed,
)


# =========================================================
# CONFIGURATION
# =========================================================

OUTPUT_DIR = "/tmp/data_source_generator"

FILES_PER_RUN = 1

POLICY_RECORDS_PER_FILE = 50

CLAIM_RECORDS_PER_FILE = 10


# =========================================================
# DEFAULT ARGS
# =========================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


# =========================================================
# SOURCE FEED GENERATOR TASK
# =========================================================

def run_data_source_generator():

    generate_data_feed(

        files_per_run=FILES_PER_RUN,

        policy_records_per_file=(
            POLICY_RECORDS_PER_FILE
        ),

        claim_records_per_file=(
            CLAIM_RECORDS_PER_FILE
        ),

        output_dir=OUTPUT_DIR,
    )


# =========================================================
# DAG
# =========================================================

SCHEDULE = Variable.get("DATA_SIMULATOR_SCHEDULE", default_var=None)

with DAG(

    dag_id="insurance_feed_preparation_dag",

    default_args=default_args,

    start_date=datetime(2026, 1, 1),

    schedule=SCHEDULE,

    catchup=False,

    description=(
        "Feed File Preparation DAG for file ingestion and processing."
    ),

    tags=[
        "data-preparation"],

) as dag:

    generate_data_feeds = PythonOperator(

        task_id="generate_data_feed",

        python_callable=(
            run_data_source_generator
        ),
    )

    generate_data_feeds