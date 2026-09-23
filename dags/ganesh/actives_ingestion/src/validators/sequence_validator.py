"""Check 8: sequence number not repeated or out of order, per region.

Pure and DB-free: the caller (file_validation_pipeline) supplies the last
known sequence number for this region, seeded from
file_tracker.fetch_max_sequence_per_region() and updated in-memory after
each accepted file in the current run -- covering both cross-run and
intra-run ordering with a single mechanism.
"""

from typing import Optional

from src.config.actives_schema import RejectionReason
from src.models.validation_result import ValidationOutcome


def validate_sequence(
    region: str, sequence_number: int, last_known_sequence: Optional[int]
) -> ValidationOutcome:
    if last_known_sequence is not None and sequence_number <= last_known_sequence:
        return ValidationOutcome.fail(
            RejectionReason.SEQUENCE_INVALID,
            detail=(
                f"sequence {sequence_number} for region {region!r} is not greater than "
                f"the last processed sequence {last_known_sequence}"
            ),
        )
    return ValidationOutcome.ok()
