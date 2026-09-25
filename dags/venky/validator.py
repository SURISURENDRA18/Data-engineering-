from __future__ import annotations

import csv
import gzip
import hashlib
import io
import os
import re
from typing import Iterable

from .config import EXPECTED_COLUMNS, FILENAME_PATTERN


def parse_filename(filename: str):
    """
    Validate filename and extract:
        region
        sequence number
        compression status
    """

    match = re.fullmatch(FILENAME_PATTERN, filename)

    if not match:
        return None

    parts = filename.split("_")

    region = parts[0]
    sequence = int(parts[3])

    compressed = filename.endswith(".csv.gz")

    return {
        "region": region,
        "sequence": sequence,
        "compressed": compressed,
    }


def calculate_md5(file_path: str) -> str:
    """
    Calculate exact file checksum.
    """

    md5 = hashlib.md5()

    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            md5.update(chunk)

    return md5.hexdigest()


def open_text_file(file_path: str, compressed: bool):
    """
    Open CSV as text.

    Supports:
        .csv
        .csv.gz
    """

    if compressed:
        return gzip.open(
            file_path,
            mode="rt",
            encoding="utf-8",
            newline="",
        )

    return open(
        file_path,
        mode="r",
        encoding="utf-8",
        newline="",
    )


def validate_file(
    file_path: str,
    filename: str,
    existing_checksums: set[str],
    max_sequence_for_region: int | None,
):
    """
    Apply Task 1 validation checks in the required order.

    Returns:
        (True, None, metadata)
    or:
        (False, reason, metadata)
    """

    # ---------------------------------------------------------
    # 1. Filename pattern
    # ---------------------------------------------------------

    parsed = parse_filename(filename)

    if parsed is None:
        return False, "pattern_mismatch", None

    # ---------------------------------------------------------
    # 2. Zero size
    # ---------------------------------------------------------

    if os.path.getsize(file_path) == 0:
        return False, "zero_size", parsed

    # ---------------------------------------------------------
    # 3-6. File content checks
    # ---------------------------------------------------------

    try:
        with open_text_file(file_path, parsed["compressed"]) as file:
            reader = csv.reader(
                file,
                delimiter=";",
            )

            header = next(reader, None)

            if header is None:
                return False, "header_only", parsed

            first_data_row = next(reader, None)

            if first_data_row is None:
                return False, "header_only", parsed

            # Header count/order check
            if header != EXPECTED_COLUMNS:
                return False, "invalid_column_count", parsed

            expected_count = len(EXPECTED_COLUMNS)

            # Check first data row
            if len(first_data_row) != expected_count:
                return False, "invalid_column_count", parsed

            # Check remaining rows
            for row in reader:
                if len(row) != expected_count:
                    return False, "invalid_column_count", parsed

    except (
        UnicodeDecodeError,
        UnicodeError,
        gzip.BadGzipFile,
        OSError,
        csv.Error,
    ):
        return False, "not_text_format", parsed

    # ---------------------------------------------------------
    # 7. Exact duplicate
    # ---------------------------------------------------------

    checksum = calculate_md5(file_path)

    if checksum in existing_checksums:
        return False, "duplicate", parsed

    # ---------------------------------------------------------
    # 8. Sequence validation
    # ---------------------------------------------------------

    if (
        max_sequence_for_region is not None
        and parsed["sequence"] <= max_sequence_for_region
    ):
        return False, "sequence_invalid", parsed

    parsed["checksum"] = checksum

    return True, None, parsed