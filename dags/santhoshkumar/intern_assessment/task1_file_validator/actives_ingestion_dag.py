from collections import defaultdict
from datetime import datetime, timedelta
import logging
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add the directory containing 'src' to Python's search path
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

from src.config import PipelineConfig
from src.file_validator import FileValidator
from src.storage import StorageManager

logger = logging.getLogger(__name__)

default_args = {
    "owner": "santhosh",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 21),
    "email_on_failure": False,
    "retries": 0,
}


def validate_and_route_landing_files():
    config = PipelineConfig()
    storage = StorageManager(
        conn_id=config.MINIO_CONN_ID, bucket_name=config.BUCKET_NAME
    )

    # 1. Discover already processed files (prevents duplicate processing)
    processed_filenames = storage.get_already_processed_filenames(
        validated_prefix=config.VALIDATED_PREFIX,
        rejected_prefix=config.REJECTED_PREFIX,
    )

    # 2. List all objects in landing
    all_landing_keys = storage.list_files(config.LANDING_PREFIX)
    if not all_landing_keys:
        logger.info("No files found in landing directory: %s", config.LANDING_PREFIX)
        return

    # Track sequences per region for sequence_invalid rule
    region_sequences = defaultdict(list)

    for key in sorted(all_landing_keys):
        filename = key.split("/")[-1]

        # Skip files already processed in previous pipeline runs
        if filename in processed_filenames:
            logger.info("File %s already processed previously. Skipping.", filename)
            continue

        # Read bytes from MinIO
        raw_bytes = storage.read_file_bytes(key)

        # Execute Rules 1 through 8 in sequence
        is_valid, reject_reason = FileValidator.validate_file(
            filename=filename,
            raw_bytes=raw_bytes,
            processed_filenames=processed_filenames,
            region_sequences=region_sequences,
        )

        if is_valid:
            dest_key = f"{config.VALIDATED_PREFIX}{filename}"
            storage.copy_file(source_key=key, dest_key=dest_key)
            logger.info("PASSED validation: %s -> %s", filename, dest_key)

            # Record sequence for order checking
            parsed = FileValidator.parse_filename(filename)
            if parsed:
                region, _, _, seq_num, _ = parsed
                region_sequences[region].append(seq_num)
        else:
            dest_key = f"{config.REJECTED_PREFIX}{reject_reason}/{filename}"
            storage.copy_file(source_key=key, dest_key=dest_key)
            logger.warning(
                "REJECTED file %s. Reason: %s -> %s", filename, reject_reason, dest_key
            )

        # Mark as seen for this run
        processed_filenames.add(filename)


with DAG(
    dag_id="santhosh_actives_ingestion",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["assessment", "actives_feed"],
) as dag:

    validate_files_task = PythonOperator(
        task_id="validate_and_route_files",
        python_callable=validate_and_route_landing_files,
    )