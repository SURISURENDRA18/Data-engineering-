from collections import defaultdict
from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure Python resolves 'src' from the parent folder (task1_file_validator)
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent

if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

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

    # 1. Discover already processed files
    processed_filenames = storage.get_already_processed_filenames(
        validated_prefix=config.VALIDATED_PREFIX,
        rejected_prefix=config.REJECTED_PREFIX,
    )

    # Pre-populate region sequences from previously validated files
    region_sequences = defaultdict(list)
    already_validated_keys = storage.list_files(config.VALIDATED_PREFIX)
    for vkey in already_validated_keys:
        vfilename = vkey.split("/")[-1]
        parsed = FileValidator.parse_filename(vfilename)
        if parsed:
            reg, _, _, seq_num, _ = parsed
            region_sequences[reg].append(seq_num)

    # 2. List all objects in landing
    all_landing_keys = storage.list_files(config.LANDING_PREFIX)
    if not all_landing_keys:
        logger.info("No files found in landing directory: %s", config.LANDING_PREFIX)
        return

    for key in sorted(all_landing_keys):
        filename = key.split("/")[-1]

        # Skip files already processed
        if filename in processed_filenames:
            logger.info("File %s already processed previously. Skipping.", filename)
            continue

        raw_bytes = storage.read_file_bytes(key)
        if raw_bytes is None:
            raw_bytes = b""

        is_valid, reject_reason = FileValidator.validate_file(
            filename=filename,
            raw_bytes=raw_bytes,
            processed_filenames=processed_filenames,
            region_sequences=region_sequences,
        )

        if is_valid:
            storage.move_to_validated(
                source_key=key,
                validated_prefix=config.VALIDATED_PREFIX,
                filename=filename,
            )
            logger.info("PASSED validation: %s", filename)

            parsed = FileValidator.parse_filename(filename)
            if parsed:
                region, _, _, seq_num, _ = parsed
                region_sequences[region].append(seq_num)
        else:
            # 1. Move original file to rejected for strict spec compliance
            storage.move_to_rejected(
                source_key=key,
                rejected_prefix=config.REJECTED_PREFIX,
                reason=reject_reason,
                filename=filename,
            )
            logger.warning(
                "REJECTED file %s. Reason: %s", filename, reject_reason
            )

            # 2. Hybrid Resilient Filtering: Audit and clean rows if failure was invalid_column_count
            if reject_reason == "invalid_column_count":
                cleaned_bytes, audit_json, val_cnt, drop_cnt = (
                    FileValidator.extract_valid_rows_and_audit(
                        filename=filename, raw_bytes=raw_bytes
                    )
                )

                # Store audit manifest in rejected folder
                audit_key = f"{config.REJECTED_PREFIX}invalid_column_count/audit_{filename}.json"
                storage.upload_bytes(
                    dest_key=audit_key,
                    data=audit_json.encode("utf-8"),
                )

                # If any rows had valid 46 columns, store sanitized copy in staging/cleaned/
                if cleaned_bytes and val_cnt > 0:
                    cleaned_key = f"mediation/santhosh/staging/cleaned/{filename}"
                    storage.upload_bytes(
                        dest_key=cleaned_key,
                        data=cleaned_bytes,
                    )
                    logger.info(
                        "Resilience: Retained %d clean rows in %s (quarantined %d bad rows)",
                        val_cnt,
                        cleaned_key,
                        drop_cnt,
                    )

        processed_filenames.add(filename)


with DAG(
    dag_id="santhosh_actives_ingestion",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["assessment", "actives_feed"],
) as dag:

    validate_files_task = PythonOperator(
        task_id="validate_and_route_files",
        python_callable=validate_and_route_landing_files,
    )