"""Structured logging helpers shared by both pipeline stages.

Log lines are deliberately key=value so they stay greppable in Airflow
task logs without needing a log aggregator. No secrets are ever logged."""

import logging
from typing import Optional

from src.models.validation_result import FileValidationReport, RowLoadResult

LOGGER_NAME = "actives_ingestion"


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)


def _kv(**fields) -> str:
    parts = [f"{k}={v}" for k, v in fields.items() if v is not None]
    return " ".join(parts)


def log_file_decision(logger: logging.Logger, report: FileValidationReport) -> None:
    if report.is_valid:
        logger.info(
            "FILE_VALIDATED "
            + _kv(
                file=report.file_name,
                region=report.region,
                sequence=report.sequence_number,
                size_bytes=report.file_size_bytes,
                row_count=report.row_count,
                checksum=report.file_checksum,
                destination=report.destination_key,
            )
        )
    else:
        logger.warning(
            "FILE_REJECTED "
            + _kv(
                file=report.file_name,
                stage="file_validation",
                reason=report.outcome.reason,
                detail=report.outcome.detail,
                size_bytes=report.file_size_bytes,
                destination=report.destination_key,
            )
        )


def log_file_error(logger: logging.Logger, file_key: str, stage: str, error: Exception) -> None:
    logger.error(
        "FILE_ERROR " + _kv(file=file_key, stage=stage, error_type=type(error).__name__, error=str(error))
    )


def log_row_rejections(logger: logging.Logger, file_name: str, rejected_by_reason: dict) -> None:
    for reason, count in rejected_by_reason.items():
        logger.info("ROW_REJECTIONS " + _kv(file=file_name, reason=reason, count=count))


def log_load_result(logger: logging.Logger, result: RowLoadResult) -> None:
    logger.info(
        "FILE_LOAD_RESULT "
        + _kv(
            file=result.file_name,
            status=result.status,
            rows_read=result.total_rows,
            rows_loaded=result.loaded_rows,
            rows_rejected=result.rejected_rows,
            rows_deduplicated=result.duplicate_rows_dropped,
            error=result.error_message,
        )
    )
    if result.rejected_by_reason:
        log_row_rejections(logger, result.file_name, result.rejected_by_reason)


def log_pipeline_summary(logger: logging.Logger, title: str, summary: dict) -> None:
    logger.info("=" * 41)
    logger.info(title)
    logger.info("=" * 41)
    for key, value in summary.items():
        logger.info(_kv(**{key: value}))
