from collections import defaultdict
import csv
from datetime import datetime, timedelta
import io
import json
import logging
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Locate intern_assessment directories
CURRENT_DIR = Path(__file__).resolve().parent
ASSESSMENT_ROOT = CURRENT_DIR / "intern_assessment"

for p in [
    str(ASSESSMENT_ROOT),
    str(ASSESSMENT_ROOT / "task1_file_validator"),
    str(ASSESSMENT_ROOT / "task2_table_loader"),
]:
    if p not in sys.path:
        sys.path.insert(0, p)

from task1_file_validator.src.config import PipelineConfig
from task1_file_validator.src.file_validator import FileValidator
from task1_file_validator.src.storage import StorageManager
from task2_table_loader.src.config import LoaderConfig
from task2_table_loader.src.storage import StorageManager as DorisStorageManager
from task2_table_loader.src.table_loader import RawTableLoader

logger = logging.getLogger(__name__)

default_args = {
    "owner": "santhosh",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 21),
    "email_on_failure": False,
    "retries": 0,
}


def validate_and_route_landing_files():
    """Task 1: Validates landing files and routes to validated or rejected folders."""
    config = PipelineConfig()
    storage = StorageManager(
        conn_id=config.MINIO_CONN_ID, bucket_name=config.BUCKET_NAME
    )

    processed_filenames = storage.get_already_processed_filenames(
        validated_prefix=config.VALIDATED_PREFIX,
        rejected_prefix=config.REJECTED_PREFIX,
    )

    region_sequences = defaultdict(list)
    already_validated_keys = storage.list_files(config.VALIDATED_PREFIX)
    for vkey in already_validated_keys:
        vfilename = vkey.split("/")[-1]
        parsed = FileValidator.parse_filename(vfilename)
        if parsed:
            reg, _, _, seq_num, _ = parsed
            region_sequences[reg].append(seq_num)

    all_landing_keys = storage.list_files(config.LANDING_PREFIX)
    if not all_landing_keys:
        logger.info("No files found in landing directory: %s", config.LANDING_PREFIX)
        return

    audit_summary_records = []

    for key in sorted(all_landing_keys):
        filename = key.split("/")[-1]

        if "Active" not in filename:
            continue

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
            validated_dest_key = f"{config.VALIDATED_PREFIX.rstrip('/')}/{filename}"
            storage.upload_bytes(
                dest_key=validated_dest_key,
                data=raw_bytes,
            )
            logger.info("PASSED validation and stored copy at: %s", validated_dest_key)

            parsed = FileValidator.parse_filename(filename)
            if parsed:
                reg, _, _, seq_num, _ = parsed
                region_sequences[reg].append(seq_num)

            audit_summary_records.append({
                "Active File Name": filename,
                "Status": "PASSED",
                "Total Scanned Rows": "ALL",
                "Valid Rows Retained": "ALL",
                "Dropped Rows": 0,
                "Observed Field Count": len(config.EXPECTED_HEADERS) if hasattr(config, "EXPECTED_HEADERS") else 46,
                "Reason": "N/A"
            })
        else:
            rejected_dest_key = f"{config.REJECTED_PREFIX.rstrip('/')}/{reject_reason}/{filename}"
            storage.upload_bytes(
                dest_key=rejected_dest_key,
                data=raw_bytes,
            )
            logger.warning(
                "REJECTED file %s. Reason: %s (copied to %s)", filename, reject_reason, rejected_dest_key
            )

            if reject_reason == "invalid_column_count":
                cleaned_bytes, audit_json, val_cnt, drop_cnt = (
                    FileValidator.extract_valid_rows_and_audit(
                        filename=filename, raw_bytes=raw_bytes
                    )
                )

                audit_key = f"{config.REJECTED_PREFIX.rstrip('/')}/invalid_column_count/audit_{filename}.json"
                storage.upload_bytes(
                    dest_key=audit_key,
                    data=audit_json.encode("utf-8"),
                )

                if cleaned_bytes and val_cnt > 0:
                    cleaned_key = f"{config.VALIDATED_PREFIX.rstrip('/')}/{filename}"
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

                manifest_data = json.loads(audit_json)
                sample = manifest_data.get("dropped_details_sample", [])
                observed_cols = sample[0].get("field_count", "N/A") if sample else "N/A"

                audit_summary_records.append({
                    "Active File Name": filename,
                    "Status": "REJECTED_WITH_RESILIENCE",
                    "Total Scanned Rows": manifest_data.get("total_scanned_rows", val_cnt + drop_cnt),
                    "Valid Rows Retained": val_cnt,
                    "Dropped Rows": drop_cnt,
                    "Observed Field Count": observed_cols,
                    "Reason": reject_reason
                })
            else:
                audit_summary_records.append({
                    "Active File Name": filename,
                    "Status": "REJECTED",
                    "Total Scanned Rows": 0,
                    "Valid Rows Retained": 0,
                    "Dropped Rows": 0,
                    "Observed Field Count": "N/A",
                    "Reason": reject_reason
                })

        processed_filenames.add(filename)

    if audit_summary_records:
        csv_buffer = io.StringIO()
        fieldnames = [
            "Active File Name",
            "Status",
            "Total Scanned Rows",
            "Valid Rows Retained",
            "Dropped Rows",
            "Observed Field Count",
            "Reason"
        ]
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        for rec in audit_summary_records:
            writer.writerow(rec)

        audit_target_key = f"{getattr(config, 'AUDIT_PREFIX', 'mediation/santhosh/audit/').rstrip('/')}/audit_summary.csv"
        storage.upload_bytes(
            dest_key=audit_target_key,
            data=csv_buffer.getvalue().encode("utf-8"),
        )
        logger.info("Uploaded tabular audit summary to: %s", audit_target_key)


def run_load_raw_table(**context):
    """Task 2: Loads validated records into Apache Doris."""
    config = LoaderConfig()
    storage = DorisStorageManager(
        conn_id=config.minio_conn_id,
        bucket_name=config.bucket_name
    )
    loader = RawTableLoader(config=config)

    loader.create_tables_if_not_exist()

    file_keys = storage.list_files(prefix=config.source_prefix)
    if not file_keys:
        logger.info("No validated files found under prefix: %s", config.source_prefix)
        return

    logger.info("Found %d file(s) under prefix: %s", len(file_keys), config.source_prefix)

    total_loaded = 0
    total_rejected = 0

    for key in sorted(file_keys):
        file_name = key.split("/")[-1]

        if not file_name.endswith((".csv", ".csv.gz")):
            continue

        if loader.is_file_processed(file_name=file_name):
            logger.info("File already loaded in earlier run, skipping: %s", file_name)
            continue

        logger.info("Processing file: %s", file_name)
        raw_bytes = storage.read_bytes(key=key)
        load_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        try:
            df = loader.parse_content(file_name=file_name, raw_bytes=raw_bytes)
            valid_df, rejected_rows = loader.validate_and_filter(
                df=df,
                file_name=file_name,
                load_timestamp=load_timestamp
            )
            loader.load_records(
                valid_df=valid_df,
                rejected_rows=rejected_rows,
                file_name=file_name,
                load_timestamp=load_timestamp
            )
            total_loaded += len(valid_df)
            total_rejected += len(rejected_rows)
        except Exception as e:
            logger.error("Error processing file %s: %s", file_name, str(e))
            raise e

    logger.info("All files processed. Total valid rows: %d, Total rejected: %d", total_loaded, total_rejected)


with DAG(
    dag_id="actives_ingestion_dag",
    default_args=default_args,
    description="Actives Ingestion Pipeline: validate_and_reject, then load_raw_table",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["assessment", "actives_feed"],
) as dag:

    validate_and_reject_task = PythonOperator(
        task_id="validate_and_reject",
        python_callable=validate_and_route_landing_files,
    )

    load_raw_table_task = PythonOperator(
        task_id="load_raw_table",
        python_callable=run_load_raw_table,
    )

    validate_and_reject_task >> load_raw_table_task