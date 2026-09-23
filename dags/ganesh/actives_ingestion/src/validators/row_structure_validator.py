"""Check 6: every data row has the same field count as the header.

Uses csv.reader (quote-aware) rather than pandas.read_csv for this check
specifically -- pandas silently NaN-pads/truncates short or long rows
instead of raising, which would defeat the point of this check.
"""

import csv
import io
from typing import List

from src.config.actives_schema import RejectionReason
from src.models.validation_result import ValidationOutcome


def validate_row_field_counts(text: str, delimiter: str, expected_count: int) -> ValidationOutcome:
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    next(reader, None)  # skip header, already validated separately

    for row_number, row in enumerate(reader, start=2):  # 1 = header
        if not row:
            continue
        if len(row) != expected_count:
            return ValidationOutcome.fail(
                RejectionReason.INVALID_COLUMN_COUNT,
                detail=f"row {row_number} has {len(row)} fields, expected {expected_count}",
            )
    return ValidationOutcome.ok()


def parse_data_rows(text: str, delimiter: str) -> List[List[str]]:
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    next(reader, None)
    return [row for row in reader if row]


def count_data_rows(text: str, delimiter: str) -> int:
    return len(parse_data_rows(text, delimiter))
