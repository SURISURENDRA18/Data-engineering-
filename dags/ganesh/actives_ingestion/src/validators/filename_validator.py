"""Check 1: filename pattern."""

from typing import Tuple

from src.config.actives_schema import FILENAME_PATTERN, RejectionReason
from src.models.validation_result import ValidationOutcome


def validate_filename(filename: str) -> ValidationOutcome:
    if FILENAME_PATTERN.fullmatch(filename):
        return ValidationOutcome.ok()
    return ValidationOutcome.fail(
        RejectionReason.PATTERN_MISMATCH,
        detail=f"{filename!r} does not match the required Actives filename pattern",
    )


def extract_region_and_sequence(filename: str) -> Tuple[str, int]:
    """Only safe to call after validate_filename() has passed -- assumes
    the standard REGION_INSTANCE_(Active|Actives)_SEQUENCE_TIMESTAMP shape."""
    stem = filename.split(".", 1)[0]
    parts = stem.split("_")
    region, sequence_number = parts[0], int(parts[3])
    return region, sequence_number
