"""File Handler for Task 1 (validate_and_reject).

Scans MinIO landing prefix, performs Checks 1-8 in strict order,
and safely moves files to either validated or rejected/<reason> folders.
Implements per-file isolation: failure of one file never stops subsequent files.
"""

import os
from collections.abc import Callable

from actives_ingestion.config import PipelineConfig, pipeline_config
from actives_ingestion.constants import REASON_DUPLICATE
from actives_ingestion.minio_client import MinioService
from actives_ingestion.models import PipelineSummary, ValidationResult
from actives_ingestion.repositories.file_tracking_repository import (
    FileTrackingRepository,
)
from actives_ingestion.utils import get_logger
from actives_ingestion.validators import FileValidator

logger = get_logger("file_handler")


class FileHandler:
    """Orchestrates file discovery, validation, and object storage routing."""

    def __init__(
        self,
        config: PipelineConfig | None = None,
        minio_service: MinioService | None = None,
        tracking_repository: FileTrackingRepository | None = None,
        file_validator: FileValidator | None = None,
    ):
        self.config = config or pipeline_config
        self.minio = minio_service or MinioService(self.config.minio)
        self.tracking_repo = tracking_repository or FileTrackingRepository(self.config.doris)
        self.validator = file_validator or FileValidator(self.tracking_repo)

    def process_landing_files(
        self, file_filter: Callable[[str], bool] | None = None
    ) -> PipelineSummary:
        """Scan landing folder and route every file independently.

        Args:
            file_filter: Optional predicate to filter files by filename (used in integration testing).
        """
        summary = PipelineSummary()
        self.minio.ensure_bucket_exists()
        self.validator.reset_batch_state()

        landing_files = self.minio.list_landing_files()
        if file_filter:
            landing_files = [f for f in landing_files if file_filter(os.path.basename(f))]

        summary.files_discovered = len(landing_files)
        logger.info(f"Discovered {len(landing_files)} file(s) in landing folder.")

        for object_key in landing_files:
            file_name = os.path.basename(object_key)
            try:
                self._process_single_landing_file(object_key, file_name, summary)
            except Exception as e:
                logger.exception(f"Unexpected error processing landing file '{file_name}': {e}")
                summary.files_failed += 1
                continue

        logger.info("Landing validation task complete.")
        return summary

    def _process_single_landing_file(
        self, object_key: str, file_name: str, summary: PipelineSummary
    ) -> None:
        """Process an individual landing file with strict check sequence and safe movement."""
        file_size = self.minio.get_object_size(object_key)
        stream = self.minio.get_object_stream(object_key)

        result: ValidationResult = self.validator.validate(
            filename=file_name,
            content_stream=stream,
            file_size=file_size,
        )

        file_hash_str = result.file_hash or "N/A"

        if result.is_valid:
            dest = self.minio.move_to_validated(object_key)
            summary.files_validated += 1
            logger.info(
                f"[VALIDATED] File: '{file_name}' | Hash: {file_hash_str} | Destination: '{dest}'"
            )
        else:
            reason = result.reason or "unknown_rejection"
            if reason == REASON_DUPLICATE:
                summary.files_skipped_duplicate += 1
            summary.files_rejected += 1

            dest = self.minio.move_to_rejected(object_key, reason)
            logger.warning(
                f"[REJECTED] File: '{file_name}' | Reason: '{reason}' | Hash: {file_hash_str} | Destination: '{dest}'"
            )
