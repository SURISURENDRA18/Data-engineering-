"""Task 1 orchestration: run the 8 file-level checks, in order, against
every file in the landing prefix, copying each to validated/ or
rejected/<reason>/.

Non-destructive by design: landing/ is a shared pool that other
candidates' pipelines in this environment also read from concurrently,
so files are COPIED to their destination and never deleted from
landing/. Idempotency is therefore based on "does a file with this name
already exist under validated/ or rejected/<reason>/", not on "is it
still in landing/" -- see list_processed_filenames().

Scope note: only filenames containing "Active" (covers both Active and
Actives) are considered at all -- everything else in the shared landing
zone belongs to other feeds/candidates and is left completely alone,
never read or copied.

Efficiency note: checks run strictly in order and stop at the first
failure, so a file that fails check 1 (filename) is rejected on its
*key* alone -- no MinIO GetObject, no checksum.

Tracking-table note: only files that pass checks 1-6 reach check 7
(duplicate), which is the first point a checksum is computed -- that's
where content-identity (not just filename) starts to matter.
"""

import posixpath
from typing import Dict, Optional

from src.clients.minio_client import list_objects, list_processed_filenames, read_object_bytes, route_file
from src.config.actives_schema import CSV_DELIMITER, EXPECTED_COLUMN_COUNT, RejectionReason
from src.config.settings import Settings
from src.models.validation_result import FileValidationReport, ValidationOutcome
from src.tracking.file_tracker import (
    ensure_tracking_table_exists,
    fetch_max_sequence_per_region,
    get_tracking_row,
    record_validation_result,
)
from src.utils.checksum import compute_sha256
from src.utils.file_utils import build_rejected_key, build_validated_key
from src.utils.logging_utils import get_logger, log_file_decision, log_file_error, log_pipeline_summary
from src.validators.file_content_validator import (
    DecompressionError,
    decompress_if_gzip,
    validate_has_data_rows,
    validate_is_text,
    validate_non_empty,
)
from src.validators.filename_validator import extract_region_and_sequence, validate_filename
from src.validators.header_validator import parse_header, validate_header_columns
from src.validators.row_structure_validator import count_data_rows, validate_row_field_counts
from src.validators.sequence_validator import validate_sequence


def run_validate_and_reject(s3_client, conn, settings: Settings) -> dict:
    logger = get_logger()
    ensure_tracking_table_exists(conn, settings)

    all_objects = list_objects(s3_client, settings.bucket, settings.landing_prefix)
    processed_filenames = list_processed_filenames(
        s3_client, settings.bucket, settings.validated_prefix, settings.rejected_prefix
    )

    candidates = [
        obj
        for obj in all_objects
        if "Active" in posixpath.basename(obj["key"])  # scope: ignore other feeds/candidates entirely
        and posixpath.basename(obj["key"]) not in processed_filenames  # idempotency: already routed
    ]
    candidates.sort(key=lambda o: _sequence_sort_key(o["key"]))

    sequence_baseline: Dict[str, int] = dict(fetch_max_sequence_per_region(conn, settings))

    files_validated = 0
    files_rejected = 0
    rejection_counts: Dict[str, int] = {}

    for obj in candidates:
        try:
            report = _validate_one_file(s3_client, conn, settings, obj, sequence_baseline)
        except Exception as exc:  # infrastructure/unexpected error: isolate, retry next run
            log_file_error(logger, obj["key"], "file_validation", exc)
            continue

        log_file_decision(logger, report)
        if report.is_valid:
            files_validated += 1
            sequence_baseline[report.region] = report.sequence_number
        else:
            files_rejected += 1
            reason = report.outcome.reason
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

    summary = {
        "files_discovered": len(all_objects),
        "files_in_scope": len(candidates),
        "files_validated": files_validated,
        "files_rejected": files_rejected,
        **{f"rejected_{reason}": count for reason, count in rejection_counts.items()},
    }
    log_pipeline_summary(logger, "ACTIVES FILE VALIDATION SUMMARY", summary)
    return summary


def _sequence_sort_key(key: str):
    filename = posixpath.basename(key)
    outcome = validate_filename(filename)
    if outcome.passed:
        region, sequence_number = extract_region_and_sequence(filename)
        return (0, region, sequence_number, filename)
    return (1, "", 0, filename)


def _validate_one_file(
    s3_client, conn, settings: Settings, obj: dict, sequence_baseline: Dict[str, int]
) -> FileValidationReport:
    key = obj["key"]
    filename = posixpath.basename(key)
    size = obj["size"]

    outcome = validate_filename(filename)  # check 1
    if not outcome.passed:
        return _reject_without_tracking(s3_client, settings, key, filename, size, outcome)

    region, sequence_number = extract_region_and_sequence(filename)

    outcome = validate_non_empty(size)  # check 2
    if not outcome.passed:
        return _reject_without_tracking(s3_client, settings, key, filename, size, outcome, region, sequence_number)

    raw_bytes = read_object_bytes(s3_client, settings.bucket, key)

    try:
        content_bytes = decompress_if_gzip(raw_bytes, filename)
    except DecompressionError as exc:
        outcome = ValidationOutcome.fail(RejectionReason.NOT_TEXT_FORMAT, detail=f"gzip decompression failed: {exc}")
        return _reject_without_tracking(s3_client, settings, key, filename, size, outcome, region, sequence_number)

    outcome = validate_has_data_rows(content_bytes)  # check 3
    if not outcome.passed:
        return _reject_without_tracking(s3_client, settings, key, filename, size, outcome, region, sequence_number)

    outcome = validate_is_text(content_bytes)  # check 4
    if not outcome.passed:
        return _reject_without_tracking(s3_client, settings, key, filename, size, outcome, region, sequence_number)

    text = content_bytes.decode("utf-8")

    header_fields = parse_header(text, CSV_DELIMITER)
    outcome = validate_header_columns(header_fields)  # check 5
    if not outcome.passed:
        return _reject_without_tracking(s3_client, settings, key, filename, size, outcome, region, sequence_number)

    outcome = validate_row_field_counts(text, CSV_DELIMITER, EXPECTED_COLUMN_COUNT)  # check 6
    if not outcome.passed:
        return _reject_without_tracking(s3_client, settings, key, filename, size, outcome, region, sequence_number)

    checksum = compute_sha256(raw_bytes)
    existing = get_tracking_row(conn, settings, checksum)  # check 7
    if existing is not None:
        outcome = ValidationOutcome.fail(
            RejectionReason.DUPLICATE, detail=f"identical content already processed as {existing['file_name']}"
        )
        return _reject_without_tracking(s3_client, settings, key, filename, size, outcome, region, sequence_number)

    row_count = count_data_rows(text, CSV_DELIMITER)
    outcome = validate_sequence(region, sequence_number, sequence_baseline.get(region))  # check 8

    destination_key = (
        build_validated_key(key, settings.landing_prefix, settings.validated_prefix)
        if outcome.passed
        else build_rejected_key(key, settings.landing_prefix, settings.rejected_prefix_for(outcome.reason))
    )

    report = FileValidationReport(
        file_key=key,
        file_name=filename,
        file_size_bytes=size,
        file_checksum=checksum,
        region=region,
        sequence_number=sequence_number,
        outcome=outcome,
        row_count=row_count,
        destination_key=destination_key,
    )
    record_validation_result(conn, settings, report)  # tracking write before copy (crash-safe ordering)
    route_file(s3_client, settings.bucket, key, destination_key)
    return report


def _reject_without_tracking(
    s3_client,
    settings: Settings,
    key: str,
    filename: str,
    size: int,
    outcome: ValidationOutcome,
    region: Optional[str] = None,
    sequence_number: Optional[int] = None,
) -> FileValidationReport:
    destination_key = build_rejected_key(key, settings.landing_prefix, settings.rejected_prefix_for(outcome.reason))
    route_file(s3_client, settings.bucket, key, destination_key)
    return FileValidationReport(
        file_key=key,
        file_name=filename,
        file_size_bytes=size,
        file_checksum=None,
        region=region,
        sequence_number=sequence_number,
        outcome=outcome,
        row_count=None,
        destination_key=destination_key,
    )
