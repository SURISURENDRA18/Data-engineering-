"""Check 5: header has the 46 expected columns, in exact order.

A single reason (invalid_column_count) covers both a wrong column count
and correctly-counted-but-wrong/reordered names, since the spec provides
only one reason name for this check.
"""

import csv
import io
from typing import List

from src.config.actives_schema import EXPECTED_COLUMNS, RejectionReason
from src.models.validation_result import ValidationOutcome


def parse_header(text: str, delimiter: str) -> List[str]:
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    return next(reader, [])


def validate_header_columns(header_fields: List[str]) -> ValidationOutcome:
    if header_fields == EXPECTED_COLUMNS:
        return ValidationOutcome.ok()
    return ValidationOutcome.fail(
        RejectionReason.INVALID_COLUMN_COUNT,
        detail=(
            f"expected {len(EXPECTED_COLUMNS)} columns in exact order, "
            f"got {len(header_fields)} columns"
        ),
    )
