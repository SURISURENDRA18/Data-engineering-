"""Plain data containers passed between validators, pipelines, and the
tracking/loader layers. No I/O and no business logic here."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class ValidationOutcome:
    """Result of a single check. ``passed=False`` always carries a reason."""

    passed: bool
    reason: Optional[str] = None
    detail: Optional[str] = None

    @staticmethod
    def ok() -> "ValidationOutcome":
        return ValidationOutcome(passed=True)

    @staticmethod
    def fail(reason: str, detail: Optional[str] = None) -> "ValidationOutcome":
        return ValidationOutcome(passed=False, reason=reason, detail=detail)


@dataclass
class FileValidationReport:
    """Outcome of running all 8 file-level checks against one landing file."""

    file_key: str
    file_name: str
    file_size_bytes: int
    file_checksum: Optional[str] = None
    region: Optional[str] = None
    sequence_number: Optional[int] = None
    outcome: ValidationOutcome = field(default_factory=ValidationOutcome.ok)
    row_count: Optional[int] = None
    destination_key: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.outcome.passed


@dataclass
class RowLoadResult:
    """Outcome of loading one validated file's rows into the raw table."""

    file_name: str
    total_rows: int = 0
    loaded_rows: int = 0
    rejected_rows: int = 0
    duplicate_rows_dropped: int = 0
    rejected_by_reason: dict = field(default_factory=dict)
    status: str = "LOADED"
    error_message: Optional[str] = None
