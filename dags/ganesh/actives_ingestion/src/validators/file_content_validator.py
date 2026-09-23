"""Checks 2-4: size, has-data-rows, and readable-text.

Ordering note (documented in README): check 3 ("has a data row") is
required by the spec to run before check 4 ("is readable text"). For a
.gz file we must decompress before either check can meaningfully run --
if decompression itself fails (corrupted gzip wrapper), the file cannot
be evaluated for row content at all, so that failure is reported as
not_text_format (check 4's reason) even though it is detected ahead of
check 3. This is the one place check order deviates from the literal
numbering, and is called out as an explicit assumption.
"""

import gzip

from src.config.actives_schema import RejectionReason
from src.models.validation_result import ValidationOutcome
from src.utils.file_utils import is_gzip_filename

_MIN_PRINTABLE_RATIO = 0.95


class DecompressionError(Exception):
    pass


def decompress_if_gzip(raw_bytes: bytes, filename: str) -> bytes:
    if not is_gzip_filename(filename):
        return raw_bytes
    try:
        return gzip.decompress(raw_bytes)
    except (OSError, EOFError) as exc:
        raise DecompressionError(str(exc)) from exc


def validate_non_empty(size_bytes: int) -> ValidationOutcome:
    if size_bytes > 0:
        return ValidationOutcome.ok()
    return ValidationOutcome.fail(RejectionReason.ZERO_SIZE, detail=f"file size is {size_bytes} bytes")


def validate_has_data_rows(content_bytes: bytes) -> ValidationOutcome:
    """Byte-level line count -- deliberately does not require valid text
    decoding, so a binary file with 2+ "lines" still reaches check 4."""
    lines = [line for line in content_bytes.splitlines() if line.strip()]
    if len(lines) >= 2:
        return ValidationOutcome.ok()
    return ValidationOutcome.fail(
        RejectionReason.HEADER_ONLY, detail=f"found {len(lines)} non-blank line(s), need header + >=1 data row"
    )


def validate_is_text(content_bytes: bytes) -> ValidationOutcome:
    try:
        decoded = content_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        return ValidationOutcome.fail(RejectionReason.NOT_TEXT_FORMAT, detail=f"not valid UTF-8: {exc}")

    if not decoded:
        return ValidationOutcome.fail(RejectionReason.NOT_TEXT_FORMAT, detail="empty after decode")

    printable = sum(1 for ch in decoded if ch.isprintable() or ch in "\n\r\t")
    ratio = printable / len(decoded)
    if ratio < _MIN_PRINTABLE_RATIO:
        return ValidationOutcome.fail(
            RejectionReason.NOT_TEXT_FORMAT, detail=f"printable-character ratio {ratio:.2%} below threshold"
        )
    return ValidationOutcome.ok()
