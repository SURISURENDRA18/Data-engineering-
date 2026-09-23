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

    # 1. Discover already processed files to avoid re-processing the same landing files
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

    audit_summary_records = []

    for key in sorted(all_landing_keys):
        filename = key.split("/")[-1]

        # Ignore files that do not belong to the Actives feed
        if "Active" not in filename:
            continue

        # Skip files already processed (landing original remains intact)
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
            # Copy valid data to validated prefix without deleting from landing
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
            # Save copy of rejected file without deleting original from landing
            rejected_dest_key = f"{config.REJECTED_PREFIX.rstrip('/')}/{reject_reason}/{filename}"
            storage.upload_bytes(
                dest_key=rejected_dest_key,
                data=raw_bytes,
            )
            logger.warning(
                "REJECTED file %s. Reason: %s (copied to %s)", filename, reject_reason, rejected_dest_key
            )

            # Resilient Row Filtering if invalid_column_count
            if reject_reason == "invalid_column_count":
                cleaned_bytes, audit_json, val_cnt, drop_cnt = (
                    FileValidator.extract_valid_rows_and_audit(
                        filename=filename, raw_bytes=raw_bytes
                    )
                )

                # Upload JSON manifest to rejected prefix
                audit_key = f"{config.REJECTED_PREFIX.rstrip('/')}/invalid_column_count/audit_{filename}.json"
                storage.upload_bytes(
                    dest_key=audit_key,
                    data=audit_json.encode("utf-8"),
                )

                # Route cleaned valid rows directly to validated/
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

    # 3. Generate tabular audit_summary.csv and upload to crushftp/mediation/santhosh/audit/
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