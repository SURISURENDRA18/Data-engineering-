"""File Handler for Task 1 (validate_and_reject).

Scans MinIO landing prefix, performs Checks 1-8 in strict order,
and safely copies files to either validated or rejected/<reason> folders.
Implements per-file isolation: failure of one file never stops subsequent files.
Optimized for high-volume landings with metadata caching, deferred downloads,
concurrent movements, and clean progress logging.
"""

import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        max_workers: int = 12,
    ):
        self.config = config or pipeline_config
        self.minio = minio_service or MinioService(self.config.minio)
        self.tracking_repo = tracking_repository or FileTrackingRepository(self.config.doris)
        self.validator = file_validator or FileValidator(self.tracking_repo)
        self.max_workers = max(1, max_workers)

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

        total_files = len(landing_files)
        summary.files_discovered = total_files
        logger.info(f"Discovered {total_files} file(s) in landing folder.")

        if total_files == 0:
            logger.info("Landing validation task complete (no files to process).")
            return summary

        # Pre-fetch existing routed destinations for destination-exists idempotency check
        existing_destinations = self.minio.get_destination_files()

        # Phase 1: Sequential validation (strict Check 1-8 order and state tracking)
        # Using a lazy stream getter avoids downloading byte streams for non-matching files.
        pending_moves: list[tuple[str, str, ValidationResult]] = []

        for idx, object_key in enumerate(landing_files, 1):
            file_name = os.path.basename(object_key)

            # Idempotency check: Skip if already routed to validated/ or rejected/
            if file_name in existing_destinations:
                logger.info(
                    "Landing file '%s' already routed to '%s'. Skipping to preserve idempotency.",
                    file_name,
                    existing_destinations[file_name],
                )
                summary.files_skipped_duplicate += 1
                continue

            try:
                file_size = self.minio.get_object_size(object_key)
                def stream_loader(k=object_key):
                    return self.minio.get_object_stream(k)

                result: ValidationResult = self.validator.validate(
                    filename=file_name,
                    content_stream=stream_loader,
                    file_size=file_size,
                )
                pending_moves.append((object_key, file_name, result))
            except Exception as e:
                logger.error(f"Unexpected error validating landing file '{file_name}': {e}")
                summary.files_failed += 1
                continue

            if idx % 500 == 0 or idx == total_files:
                logger.info(f"Validated {idx}/{total_files} file headers/metadata...")

        # Phase 2: Execute non-destructive copies (concurrently for live MinIO).
        if self.minio.in_memory or self.max_workers <= 1:
            for idx, (object_key, file_name, result) in enumerate(pending_moves, 1):
                try:
                    self._execute_single_move(object_key, file_name, result, summary)
                except Exception as e:
                    logger.error(f"Error routing landing file '{file_name}': {e}")
                    summary.files_failed += 1
                if idx % 100 == 0 or idx == len(pending_moves):
                    self._log_progress(summary, total_files)
        else:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {
                    executor.submit(self._move_file_worker, obj_key, fname, res): (obj_key, fname, res)
                    for obj_key, fname, res in pending_moves
                }

                completed_count = 0
                for future in as_completed(future_to_file):
                    completed_count += 1
                    _, fname, res = future_to_file[future]
                    try:
                        action, reason = future.result()
                        if action == "VALIDATED":
                            summary.files_validated += 1
                        elif action == "SKIPPED":
                            summary.files_skipped_duplicate += 1
                        else:
                            summary.files_rejected += 1
                    except Exception as e:
                        logger.error(f"Error routing landing file '{fname}': {e}")
                        summary.files_failed += 1

                    if completed_count % 100 == 0 or completed_count == len(pending_moves):
                        self._log_progress(summary, total_files)

        logger.info("Landing validation task complete.")
        return summary

    def _execute_single_move(
        self, object_key: str, file_name: str, result: ValidationResult, summary: PipelineSummary
    ) -> None:
        """Copy a single file synchronously and update summary."""
        file_hash_str = result.file_hash or "N/A"
        if result.is_valid:
            dest = self.minio.move_to_validated(object_key)
            summary.files_validated += 1
            logger.debug(
                f"[VALIDATED] File: '{file_name}' | Hash: {file_hash_str} | Destination: '{dest}'"
            )
        else:
            reason = result.reason or "unknown_rejection"
            summary.files_rejected += 1
            dest = self.minio.move_to_rejected(object_key, reason)
            logger.debug(
                f"[REJECTED] File: '{file_name}' | Reason: '{reason}' | Hash: {file_hash_str} | Destination: '{dest}'"
            )

    def _move_file_worker(
        self, object_key: str, file_name: str, result: ValidationResult
    ) -> tuple[str, str | None]:
        """Worker function for concurrent non-destructive routing."""
        file_hash_str = result.file_hash or "N/A"
        if result.is_valid:
            dest = self.minio.move_to_validated(object_key)
            logger.debug(
                f"[VALIDATED] File: '{file_name}' | Hash: {file_hash_str} | Destination: '{dest}'"
            )
            return "VALIDATED", None
        else:
            reason = result.reason or "unknown_rejection"
            dest = self.minio.move_to_rejected(object_key, reason)
            logger.debug(
                f"[REJECTED] File: '{file_name}' | Reason: '{reason}' | Hash: {file_hash_str} | Destination: '{dest}'"
            )
            return "REJECTED", reason

    @staticmethod
    def _log_progress(summary: PipelineSummary, total: int) -> None:
        """Emit clean structured progress log."""
        processed = (
            summary.files_validated
            + summary.files_rejected
            + summary.files_skipped_duplicate
            + summary.files_failed
        )
        logger.info(
            f"Routing progress: {processed}/{total} files processed "
            f"({summary.files_validated} validated, {summary.files_rejected} rejected, "
            f"{summary.files_skipped_duplicate} skipped duplicate/already-routed, "
            f"{summary.files_failed} failed)..."
        )
