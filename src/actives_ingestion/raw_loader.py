"""Raw Loader for Task 2 (load_raw_table).

Reads validated files from MinIO, applies row-level validation and deduplication,
streams bulk loads into Doris raw_actives_daily, persists rejected rows to
raw_actives_daily_rejected, and manages file-level idempotency via actives_file_tracking.
"""

import csv
import gzip
import io
import os
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from actives_ingestion.config import PipelineConfig, pipeline_config
from actives_ingestion.constants import (
    CSV_TO_DORIS_COLUMN_MAPPING,
    EXPECTED_COLUMN_COUNT,
    STATUS_FAILED,
    STATUS_PROCESSING,
    STATUS_SUCCESS,
)
from actives_ingestion.doris_client import DorisClient
from actives_ingestion.minio_client import MinioService
from actives_ingestion.models import (
    FileProcessingSummary,
    PipelineSummary,
    RejectedRow,
    RowValidationResult,
)
from actives_ingestion.repositories.file_tracking_repository import (
    FileTrackingRepository,
)
from actives_ingestion.repositories.reject_repository import RejectRepository
from actives_ingestion.utils import calculate_sha256, get_logger
from actives_ingestion.validators import (
    FilenameValidator,
    RowDeduplicator,
    RowValidator,
)

logger = get_logger("raw_loader")


class RawLoader:
    """Executes Task 2: validated file loading, row validation, deduplication, and Doris insertion."""

    def __init__(
        self,
        config: PipelineConfig | None = None,
        minio_service: MinioService | None = None,
        doris_client: DorisClient | None = None,
        tracking_repository: FileTrackingRepository | None = None,
        reject_repository: RejectRepository | None = None,
    ):
        self.config = config or pipeline_config
        self.minio = minio_service or MinioService(self.config.minio)
        self.doris = doris_client or DorisClient(self.config.doris)
        self.tracking_repo = tracking_repository or FileTrackingRepository(self.config.doris)
        self.reject_repo = reject_repository or RejectRepository(self.config.doris)

    def load_all_validated_files(
        self, file_filter: Callable[[str], bool] | None = None
    ) -> PipelineSummary:
        """Scan validated folder, load each file independently, and return summary metrics.

        Args:
            file_filter: Optional predicate to filter files by filename (used in integration testing).
        """
        summary = PipelineSummary()
        self.doris.init_database_and_tables()

        validated_files = self.minio.list_validated_files()
        if file_filter:
            validated_files = [f for f in validated_files if file_filter(os.path.basename(f))]

        summary.files_discovered = len(validated_files)
        logger.info(f"Discovered {len(validated_files)} validated file(s) ready for ingestion.")

        for object_key in validated_files:
            file_name = os.path.basename(object_key)
            try:
                file_summary = self._load_single_file(object_key, file_name)
                if file_summary.status == STATUS_SUCCESS:
                    summary.files_loaded += 1
                    summary.total_rows_loaded += file_summary.rows_loaded
                    summary.total_rows_rejected += file_summary.rows_rejected
                elif file_summary.status == "SKIPPED":
                    summary.files_skipped_duplicate += 1
                else:
                    summary.files_failed += 1
            except Exception as e:
                logger.exception(f"Fatal error loading validated file '{file_name}': {e}")
                summary.files_failed += 1
                try:
                    stream = self.minio.get_object_stream(object_key)
                    f_hash = calculate_sha256(stream)
                    self.tracking_repo.record_status(
                        file_hash=f_hash,
                        file_name=file_name,
                        status=STATUS_FAILED,
                        error_message=str(e),
                    )
                except Exception:
                    pass
                continue

        summary.log_summary(logger.info)
        return summary

    def _load_single_file(self, object_key: str, file_name: str) -> FileProcessingSummary:
        """Process and ingest a single validated file with full row isolation and idempotency."""
        stream = self.minio.get_object_stream(object_key)
        file_hash = calculate_sha256(stream)

        # Idempotency check: skip already loaded files
        if self.tracking_repo.is_file_processed(file_hash):
            logger.info(f"File already processed: {file_name} (hash: {file_hash}). Skipping.")
            return FileProcessingSummary(
                file_name=file_name,
                file_hash=file_hash,
                status="SKIPPED",
            )

        _, metadata = FilenameValidator.validate(file_name)
        region = metadata.region if metadata else None
        sequence_number = metadata.sequence_number if metadata else None

        self.tracking_repo.record_status(
            file_hash=file_hash,
            file_name=file_name,
            region=region,
            sequence_number=sequence_number,
            status=STATUS_PROCESSING,
        )

        is_gz = file_name.endswith(".gz")
        text_stream = self._get_text_stream(stream, is_gz)

        # Auto-detect delimiter: check first line
        sample_line = text_stream.readline()
        delimiter = ";" if ";" in sample_line and len(sample_line.split(";")) == EXPECTED_COLUMN_COUNT else ","
        text_stream.seek(0)

        reader = csv.reader(text_stream, delimiter=delimiter)

        header = next(reader)
        loaded_at_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        accepted_raw_rows: list[dict[str, str]] = []
        rejected_records: list[RejectedRow] = []

        row_index = 0
        for row in reader:
            row_index += 1
            raw_line = delimiter.join(row)
            row_dict = dict(zip(header, row))

            val_result: RowValidationResult = RowValidator.validate_row(
                row_dict=row_dict,
                row_number=row_index,
                raw_row=raw_line,
            )

            if val_result.is_valid:
                accepted_raw_rows.append(row_dict)
            else:
                rejected_records.append(
                    RejectedRow(
                        id=str(uuid.uuid4()),
                        file_name=file_name,
                        file_hash=file_hash,
                        row_number=row_index,
                        reason=val_result.reason or "UNKNOWN",
                        iccid=val_result.iccid,
                        customernumber=val_result.customernumber,
                        companyid=val_result.companyid,
                        raw_row=raw_line,
                        rejected_at=datetime.now(timezone.utc),
                    )
                )

        # Deduplicate accepted rows: keep latest occurrence for each business key pair
        deduplicated_rows = RowDeduplicator.deduplicate(accepted_raw_rows)

        doris_rows: list[dict[str, Any]] = []
        for row in deduplicated_rows:
            doris_row: dict[str, Any] = {
                "file_name": file_name,
                "loaded_at": loaded_at_str,
            }
            for csv_col, val in row.items():
                target_col = CSV_TO_DORIS_COLUMN_MAPPING.get(csv_col)
                if target_col:
                    doris_row[target_col] = val
            doris_rows.append(doris_row)

        rows_loaded = 0
        if doris_rows:
            rows_loaded = self.doris.stream_load_raw(doris_rows)

        rows_rejected = 0
        if rejected_records:
            rows_rejected = self.reject_repo.insert_batch(rejected_records)

        self.tracking_repo.record_status(
            file_hash=file_hash,
            file_name=file_name,
            region=region,
            sequence_number=sequence_number,
            status=STATUS_SUCCESS,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
        )

        logger.info(
            f"Successfully loaded file '{file_name}': {rows_loaded} rows loaded, {rows_rejected} rows rejected."
        )

        return FileProcessingSummary(
            file_name=file_name,
            file_hash=file_hash,
            status=STATUS_SUCCESS,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
        )

    def _get_text_stream(self, stream: io.BufferedIOBase, is_gzipped: bool) -> io.TextIOBase:
        """Wrap seekable byte stream into text reader."""
        if stream.seekable():
            stream.seek(0)
        if is_gzipped:
            gz_file = gzip.GzipFile(fileobj=stream, mode="rb")
            return io.TextIOWrapper(gz_file, encoding="utf-8", errors="strict")
        return io.TextIOWrapper(stream, encoding="utf-8", errors="strict")
