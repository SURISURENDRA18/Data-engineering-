"""Sole owner of reads/writes to the shared ``file_processing_log`` control
table. Task 1 (duplicate + sequence checks) and Task 2 (load idempotency)
both go through this module rather than issuing their own SQL.
"""

from datetime import datetime, timezone
from typing import Dict, Optional

from src.clients.doris_client import execute_query, execute_write, render_sql, run_ddl_script
from src.config.actives_schema import LoadStatus, ValidationStatus
from src.config.settings import Settings
from src.models.validation_result import FileValidationReport


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_tracking_table_exists(conn, settings: Settings) -> None:
    ddl = render_sql("ddl/003_create_file_processing_log.sql", DORIS_DATABASE=settings.doris_database)
    run_ddl_script(conn, ddl)


def get_tracking_row(conn, settings: Settings, file_checksum: str) -> Optional[dict]:
    sql = render_sql("queries/check_file_tracking_by_checksum.sql", DORIS_DATABASE=settings.doris_database)
    rows = execute_query(conn, sql, (file_checksum,))
    return rows[0] if rows else None


def get_load_status(conn, settings: Settings, file_checksum: str) -> Optional[str]:
    row = get_tracking_row(conn, settings, file_checksum)
    return row["load_status"] if row else None


def fetch_max_sequence_per_region(conn, settings: Settings) -> Dict[str, int]:
    sql = render_sql("queries/fetch_max_sequence_per_region.sql", DORIS_DATABASE=settings.doris_database)
    rows = execute_query(conn, sql)
    return {row["region"]: row["max_sequence_number"] for row in rows if row["region"]}


def _upsert(conn, settings: Settings, values: tuple) -> None:
    sql = render_sql("queries/upsert_file_tracking_record.sql", DORIS_DATABASE=settings.doris_database)
    execute_write(conn, sql, values)


def record_validation_result(conn, settings: Settings, report: FileValidationReport) -> None:
    """Writes the tracking row for a just-validated/rejected file. Called
    *before* the corresponding MinIO copy (route_file), so a crash in
    between leaves a resumable, non-ambiguous state: the original in
    landing/ is untouched either way (copying is non-destructive), and a
    rerun's filename-based skip (list_processed_filenames) or this row's
    presence both correctly identify the file as already handled."""
    now = _now()
    is_valid = report.is_valid
    values = (
        report.file_checksum,
        report.file_name,
        report.region,
        report.sequence_number,
        report.file_key,
        report.destination_key,
        ValidationStatus.VALIDATED if is_valid else ValidationStatus.REJECTED,
        None if is_valid else report.outcome.reason,
        None if is_valid else report.outcome.detail,
        LoadStatus.PENDING if is_valid else LoadStatus.NOT_APPLICABLE,
        report.row_count,
        None,
        None,
        None,
        report.file_size_bytes,
        now,
        None,
        now,
    )
    _upsert(conn, settings, values)


def record_load_result(
    conn,
    settings: Settings,
    file_checksum: str,
    load_status: str,
    row_count_loaded: int,
    row_count_rejected: int,
    row_count_duplicate_dropped: int,
) -> None:
    """Re-reads the existing row in full (Doris's UNIQUE KEY replaces the
    entire row on INSERT, so every existing column must be carried forward
    or it would be silently blanked) and re-writes it with updated
    load-status fields. Read-modify-write rather than a partial UPDATE, to
    stay simple and version-agnostic across Doris releases."""
    existing = get_tracking_row(conn, settings, file_checksum)
    if existing is None:
        raise RuntimeError(
            f"No tracking row found for checksum={file_checksum!r}; "
            "record_validation_result must run before record_load_result."
        )
    now = _now()
    values = (
        file_checksum,
        existing["file_name"],
        existing["region"],
        existing["sequence_number"],
        existing["landing_key"],
        existing["current_key"],
        existing["validation_status"],
        existing["rejection_reason"],
        existing["validation_detail"],
        load_status,
        existing["row_count_total"],
        row_count_loaded,
        row_count_rejected,
        row_count_duplicate_dropped,
        existing["file_size_bytes"],
        existing["validated_at"],
        now if load_status == LoadStatus.LOADED else existing["loaded_at"],
        now,
    )
    _upsert(conn, settings, values)
