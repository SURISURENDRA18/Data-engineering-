"""Data models for Actives Ingestion Pipeline."""

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class FileMetadata:
    """Extracted metadata for a landing/validated file."""
    file_name: str
    object_name: str
    file_size: int
    file_hash: str | None = None
    region: str | None = None
    instance: str | None = None
    feed: str | None = None
    sequence_number: int | None = None
    timestamp_str: str | None = None
    is_gzipped: bool = False


@dataclass
class ValidationResult:
    """Outcome of file validation (Checks 1 to 8)."""
    is_valid: bool
    reason: str | None = None
    file_hash: str | None = None
    metadata: FileMetadata | None = None

    @classmethod
    def success(cls, metadata: FileMetadata, file_hash: str) -> "ValidationResult":
        return cls(is_valid=True, reason=None, file_hash=file_hash, metadata=metadata)

    @classmethod
    def failure(cls, reason: str, metadata: FileMetadata | None = None, file_hash: str | None = None) -> "ValidationResult":
        return cls(is_valid=False, reason=reason, file_hash=file_hash, metadata=metadata)


@dataclass
class RowValidationResult:
    """Outcome of single row validation."""
    is_valid: bool
    reason: str | None = None
    row_number: int = 0
    row_data: dict[str, str] | None = None
    raw_row: str = ""
    iccid: str = ""
    customernumber: str = ""
    companyid: str = ""


@dataclass
class RejectedRow:
    """Structure for rejected row persistence."""
    id: str
    file_name: str
    file_hash: str
    row_number: int
    reason: str
    iccid: str
    customernumber: str
    companyid: str
    raw_row: str
    rejected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class FileProcessingSummary:
    """Summary of processing for a single file."""
    file_name: str
    file_hash: str
    status: str
    rows_loaded: int = 0
    rows_rejected: int = 0
    error_message: str | None = None


@dataclass
class PipelineSummary:
    """Overall batch summary metrics."""
    files_discovered: int = 0
    files_validated: int = 0
    files_rejected: int = 0
    files_skipped_duplicate: int = 0
    files_loaded: int = 0
    files_failed: int = 0
    total_rows_loaded: int = 0
    total_rows_rejected: int = 0

    def log_summary(self, logger_func=print) -> None:
        """Log pipeline execution summary in a clear structured format."""
        logger_func("=" * 60)
        logger_func("PIPELINE EXECUTION SUMMARY")
        logger_func(f"  Files Discovered:           {self.files_discovered}")
        logger_func(f"  Files Validated:            {self.files_validated}")
        logger_func(f"  Files Rejected:             {self.files_rejected}")
        logger_func(f"  Files Skipped (Duplicate):  {self.files_skipped_duplicate}")
        logger_func(f"  Files Loaded:               {self.files_loaded}")
        logger_func(f"  Files Failed:               {self.files_failed}")
        logger_func(f"  Total Rows Loaded:          {self.total_rows_loaded}")
        logger_func(f"  Total Rows Rejected:        {self.total_rows_rejected}")
        logger_func("=" * 60)
