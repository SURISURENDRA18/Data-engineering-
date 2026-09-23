from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Dynamically add the task2 parent directory to sys.path so 'src' imports cleanly
CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))

# Import task-specific modules
from src.config import LoaderConfig
from src.storage import StorageManager
from src.table_loader import RawTableLoader

# Initialize module logger
logger = logging.getLogger(__name__)

# Standard Airflow task parameters
default_args = {
    "owner": "santhosh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_load_raw_table(**context):
    """Orchestrates file reading from MinIO, row-level validation, and Apache Doris loading."""
    # Instantiate configuration, storage manager, and loader
    config = LoaderConfig()
    storage = StorageManager(
        conn_id=config.minio_conn_id,
        bucket_name=config.bucket_name
    )
    loader = RawTableLoader(config=config)

    # List all files under the validated prefix
    file_keys = storage.list_files(prefix=config.source_prefix)
    if not file_keys:
        logger.info("No validated files found under prefix: %s", config.source_prefix)
        return

    logger.info("Found %d file(s) under prefix: %s", len(file_keys), config.source_prefix)

    # Process each discovered file independently
    for key in file_keys:
        file_name = key.split("/")[-1]

        # Verify idempotency via the audit table
        if loader.is_file_processed(file_name=file_name):
            logger.info("File already loaded in earlier run, skipping: %s", file_name)
            continue

        logger.info("Processing file: %s", file_name)

        # Download raw bytes from MinIO
        raw_bytes = storage.read_bytes(key=key)

        # Generate universal execution timestamp
        load_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Parse CSV content into structured DataFrame
        df = loader.parse_content(file_name=file_name, raw_bytes=raw_bytes)

        # Separate valid rows from rejected rows and apply deduplication
        valid_df, rejected_rows = loader.validate_and_filter(
            df=df,
            file_name=file_name,
            load_timestamp=load_timestamp
        )

        # Atomically write records to raw_actives, raw_actives_rejected, and loaded_files_audit
        loader.load_records(
            valid_df=valid_df,
            rejected_rows=rejected_rows,
            file_name=file_name,
            load_timestamp=load_timestamp
        )


# Instantiate the DAG
with DAG(
    dag_id="task2_load_raw_table",
    default_args=default_args,
    description="Loads validated actives records into Apache Doris raw table",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["task2", "raw_loader", "doris"],
) as dag:

    # Define the single execution task
    load_raw_table_task = PythonOperator(
        task_id="load_raw_table",
        python_callable=run_load_raw_table,
    )