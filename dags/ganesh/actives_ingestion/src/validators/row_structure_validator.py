"""Check 6: every data row has the same field count as the header.

Revised per direct feedback: a missing *trailing* field is still a valid
(implicitly empty) field, the same way two consecutive delimiters
represent an explicit empty field mid-row. Real Actives files in this
environment routinely omit trailing optional columns entirely (no
trailing delimiters at all) rather than emitting them as empty --
confirmed by inspecting the raw bytes of real files and traced to the
feed simulator's own checked-in seed sample, which has this same
characteristic. A short row is therefore padded with empty strings up
to the expected count and accepted; only a row with *more* fields than
expected (genuinely malformed, not just missing trailing optionals) is
rejected.

Uses csv.reader (quote-aware) rather than pandas.read_csv for this check
specifically -- pandas silently NaN-pads/truncates rows on its own terms
instead of raising, which would defeat the point of this check being
explicit about what it accepts and why.
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
        if len(row) > expected_count:
            return ValidationOutcome.fail(
                RejectionReason.INVALID_COLUMN_COUNT,
                detail=f"row {row_number} has {len(row)} fields, expected at most {expected_count}",
            )
    return ValidationOutcome.ok()


def pad_row(row: List[str], expected_count: int) -> List[str]:
    """Missing trailing fields are implicitly empty -- pad up to
    expected_count so every row is uniform width downstream."""
    if len(row) >= expected_count:
        return row[:expected_count]
    return row + [""] * (expected_count - len(row))


def parse_data_rows(text: str, delimiter: str, expected_count: int = None) -> List[List[str]]:
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    next(reader, None)
    rows = [row for row in reader if row]
    if expected_count is not None:
        rows = [pad_row(row, expected_count) for row in rows]
    return rows


def count_data_rows(text: str, delimiter: str) -> int:
    return len(parse_data_rows(text, delimiter))
