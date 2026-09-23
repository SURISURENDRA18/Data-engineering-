"""Validation logic for files and rows according to Assessment requirements.

Implements:
- Filename validation (Check 1)
- File content validation (Checks 2-6)
- Duplicate detection (Check 7)
- Sequence number validation (Check 8)
- Row validation (ICCID, CustomerNumber, CompanyID)
- Deduplication (latest row retention for duplicate business key pairs)
"""

import csv
import gzip
import io
import zlib
from collections.abc import Callable

from actives_ingestion.constants import (
    EXPECTED_COLUMN_COUNT,
    EXPECTED_COLUMNS,
    FILENAME_REGEX,
    REASON_DUPLICATE,
    REASON_HEADER_ONLY,
    REASON_INVALID_COLUMN_COUNT,
    REASON_NOT_TEXT_FORMAT,
    REASON_PATTERN_MISMATCH,
    REASON_SEQUENCE_INVALID,
    REASON_ZERO_SIZE,
    ROW_REASON_NULL_OR_EMPTY_CUSTOMER_NUMBER,
    ROW_REASON_NULL_OR_EMPTY_ICCID,
    ROW_REASON_NULL_OR_ZERO_COMPANY_ID,
)
from actives_ingestion.models import FileMetadata, RowValidationResult, ValidationResult
from actives_ingestion.utils import calculate_sha256, is_gzip_bytes


class FilenameValidator:
    """Validates filename pattern and extracts metadata components."""

    @staticmethod
    def validate(filename: str) -> tuple[bool, FileMetadata | None]:
        r"""Validate filename against regex and extract metadata.

        Regex: ^[A-Za-z]+_[0-9]+_(Active|Actives)_[0-9]+_[0-9]+\.csv(\.gz)?$
        Timestamp must be exactly 14 digits.
        """
        match = FILENAME_REGEX.match(filename)
        if not match:
            return False, None

        groups = match.groupdict()
        timestamp_str = groups.get("timestamp") or ""
        # 14-digit timestamp check
        if len(timestamp_str) != 14:
            return False, None

        is_gz = bool(groups.get("compression"))
        metadata = FileMetadata(
            file_name=filename,
            object_name=filename,
            file_size=0,
            region=groups.get("region"),
            instance=groups.get("instance"),
            feed=groups.get("feed"),
            sequence_number=int(groups.get("sequence")),
            timestamp_str=timestamp_str,
            is_gzipped=is_gz,
        )
        return True, metadata


class FileValidator:
    """Performs Checks 1 through 8 in strict sequence."""

    def __init__(self, tracking_repository=None):
        self.tracking_repo = tracking_repository
        self.seen_hashes: set[str] = set()
        self.seen_sequences: dict[str, set[int]] = {}

    def reset_batch_state(self) -> None:
        """Reset in-batch cache of seen file hashes and sequence numbers."""
        self.seen_hashes.clear()
        self.seen_sequences.clear()

    def validate(
        self,
        filename: str,
        content_stream: io.BufferedIOBase | Callable[[], io.BufferedIOBase] | None,
        file_size: int,
    ) -> ValidationResult:
        """Run all 8 validation checks in exact required order:

        Check 1: Filename regex -> pattern_mismatch
        Check 2: File size > 0 -> zero_size
        Check 3: Header only check -> header_only
        Check 4: Text / gzip corruption check -> not_text_format
        Check 5: Header 46 columns in exact order -> invalid_column_count
        Check 6: Data row column count -> invalid_column_count
        Check 7: Duplicate detection -> duplicate
        Check 8: Sequence number validation -> sequence_invalid
        """
        # Check 1: Filename regex
        is_valid_name, metadata = FilenameValidator.validate(filename)
        if not is_valid_name or metadata is None:
            return ValidationResult.failure(REASON_PATTERN_MISMATCH)

        metadata.file_size = file_size

        # Check 2: File size must be greater than zero
        if file_size <= 0:
            return ValidationResult.failure(REASON_ZERO_SIZE, metadata=metadata)

        # Resolve stream lazily if passed as a callable getter
        resolved_stream: io.BufferedIOBase | None = (
            content_stream() if callable(content_stream) else content_stream
        )
        if resolved_stream is None:
            return ValidationResult.failure(REASON_ZERO_SIZE, metadata=metadata)

        # Compute SHA-256 hash for file identity
        file_hash = calculate_sha256(resolved_stream)
        metadata.file_hash = file_hash

        # Check 3: determine whether at least one data record exists before
        # running the full text/format/schema validation in Check 4.
        has_data_row = self._has_data_row(resolved_stream, metadata.is_gzipped)
        if has_data_row is False:
            return ValidationResult.failure(REASON_HEADER_ONLY, metadata=metadata, file_hash=file_hash)

        # Check 4: verify text/gzip format.
        text_stream, is_text_format = self._open_text_stream(resolved_stream, metadata.is_gzipped)
        if not is_text_format or text_stream is None:

            return ValidationResult.failure(REASON_NOT_TEXT_FORMAT, metadata=metadata, file_hash=file_hash)

        try:
            # Check for binary null bytes or text decode errors in the first line
            sample_line = text_stream.readline()
            if not sample_line:
                return ValidationResult.failure(REASON_HEADER_ONLY, metadata=metadata, file_hash=file_hash)
            if "\x00" in sample_line:
                return ValidationResult.failure(REASON_NOT_TEXT_FORMAT, metadata=metadata, file_hash=file_hash)

            # Delimiter detection: support semicolon ';' (telecom standard) and comma ','
            delimiter = ";" if ";" in sample_line and len(sample_line.split(";")) == EXPECTED_COLUMN_COUNT else ","

            text_stream.seek(0)
            reader = csv.reader(text_stream, delimiter=delimiter)

            try:
                header = next(reader)
            except StopIteration:
                return ValidationResult.failure(REASON_HEADER_ONLY, metadata=metadata, file_hash=file_hash)

            # Check 3: There must be at least one data row after header
            try:
                first_row = next(reader)
            except StopIteration:
                return ValidationResult.failure(REASON_HEADER_ONLY, metadata=metadata, file_hash=file_hash)

            # Check 5: Header must contain exactly 46 columns in exact order
            if header != EXPECTED_COLUMNS:
                return ValidationResult.failure(REASON_INVALID_COLUMN_COUNT, metadata=metadata, file_hash=file_hash)

            # Check 6: Every data row must contain exactly 46 fields
            if len(first_row) != EXPECTED_COLUMN_COUNT:
                return ValidationResult.failure(REASON_INVALID_COLUMN_COUNT, metadata=metadata, file_hash=file_hash)

            for row in reader:
                if len(row) != EXPECTED_COLUMN_COUNT:
                    return ValidationResult.failure(REASON_INVALID_COLUMN_COUNT, metadata=metadata, file_hash=file_hash)

        except (UnicodeDecodeError, csv.Error, gzip.BadGzipFile, zlib.error, EOFError, ValueError):
            return ValidationResult.failure(REASON_NOT_TEXT_FORMAT, metadata=metadata, file_hash=file_hash)

        if resolved_stream and resolved_stream.seekable():
            resolved_stream.seek(0)

        # Check 7: Duplicate detection (in-batch and persistent database)
        if file_hash in self.seen_hashes:
            return ValidationResult.failure(REASON_DUPLICATE, metadata=metadata, file_hash=file_hash)

        if self.tracking_repo and self.tracking_repo.is_file_processed(file_hash):
            return ValidationResult.failure(REASON_DUPLICATE, metadata=metadata, file_hash=file_hash)

        # Check 8: Sequence number validation (in-batch and persistent database)
        if metadata.region and metadata.sequence_number is not None:
            region = metadata.region
            seq = metadata.sequence_number

            if seq < 1:
                return ValidationResult.failure(REASON_SEQUENCE_INVALID, metadata=metadata, file_hash=file_hash)

            # Check persistent repository history
            if self.tracking_repo:
                is_seq_valid = self.tracking_repo.validate_sequence(
                    region=region,
                    sequence_number=seq,
                )
                if not is_seq_valid:
                    return ValidationResult.failure(REASON_SEQUENCE_INVALID, metadata=metadata, file_hash=file_hash)

            # Check in-batch seen sequences
            batch_seqs = self.seen_sequences.get(region, set())
            if seq in batch_seqs:
                return ValidationResult.failure(REASON_SEQUENCE_INVALID, metadata=metadata, file_hash=file_hash)
            if batch_seqs and seq <= max(batch_seqs):
                return ValidationResult.failure(REASON_SEQUENCE_INVALID, metadata=metadata, file_hash=file_hash)

        # File is valid; record in batch state
        self.seen_hashes.add(file_hash)
        if metadata.region and metadata.sequence_number is not None:
            self.seen_sequences.setdefault(metadata.region, set()).add(metadata.sequence_number)

        return ValidationResult.success(metadata=metadata, file_hash=file_hash)

    def _open_text_stream(
        self, stream: io.BufferedIOBase, is_gzipped: bool
    ) -> tuple[io.TextIOBase | None, bool]:
        """Safely decompress gzip or wrap raw stream into a TextIOWrapper.

        Returns (text_stream, is_valid_text).
        """
        try:
            if stream.seekable():
                stream.seek(0)

            if is_gzipped:
                magic = stream.read(2)
                if not is_gzip_bytes(magic):
                    return None, False
                if stream.seekable():
                    stream.seek(0)

                gz_file = gzip.GzipFile(fileobj=stream, mode="rb")
                text_io = io.TextIOWrapper(gz_file, encoding="utf-8", errors="strict")
                return text_io, True
            else:
                text_io = io.TextIOWrapper(stream, encoding="utf-8", errors="strict")
                return text_io, True
        except Exception:
            return None, False

    def _has_data_row(self, stream: io.BufferedIOBase, is_gzipped: bool) -> bool | None:
        """Return False for a valid header-only file, None for format errors."""
        try:
            if stream.seekable():
                stream.seek(0)
            if is_gzipped:
                with gzip.GzipFile(fileobj=stream, mode="rb") as gz_file:
                    payload = gz_file.read()
                text = payload.decode("utf-8", errors="strict")
            else:
                text = stream.read().decode("utf-8", errors="strict")
            if "\x00" in text:
                return None
            reader = csv.reader(io.StringIO(text))
            next(reader)
            next(reader)
            return True
        except StopIteration:
            return False
        except (UnicodeDecodeError, gzip.BadGzipFile, zlib.error, EOFError, csv.Error):
            return None
        finally:
            if stream.seekable():
                stream.seek(0)


class RowValidator:
    """Row-level validation for raw data records (Task 2)."""

    @staticmethod
    def validate_row(
        row_dict: dict[str, str],
        row_number: int,
        raw_row: str = "",
    ) -> RowValidationResult:
        """Validate an individual record dictionary.

        Checks in order:
        1. ICCID is empty -> NULL_OR_EMPTY_ICCID
        2. CustomerNumber is empty -> NULL_OR_EMPTY_CUSTOMER_NUMBER
        3. CompanyID is empty OR zero -> NULL_OR_ZERO_COMPANY_ID
        """
        iccid = (row_dict.get("ICCID") or "").strip()
        customernumber = (row_dict.get("CustomerNumber") or "").strip()
        companyid = (row_dict.get("CompanyID") or "").strip()

        if not iccid:
            return RowValidationResult(
                is_valid=False,
                reason=ROW_REASON_NULL_OR_EMPTY_ICCID,
                row_number=row_number,
                row_data=row_dict,
                raw_row=raw_row,
                iccid=iccid,
                customernumber=customernumber,
                companyid=companyid,
            )

        if not customernumber:
            return RowValidationResult(
                is_valid=False,
                reason=ROW_REASON_NULL_OR_EMPTY_CUSTOMER_NUMBER,
                row_number=row_number,
                row_data=row_dict,
                raw_row=raw_row,
                iccid=iccid,
                customernumber=customernumber,
                companyid=companyid,
            )

        if not companyid or companyid == "0" or (companyid.isdigit() and int(companyid) == 0):
            return RowValidationResult(
                is_valid=False,
                reason=ROW_REASON_NULL_OR_ZERO_COMPANY_ID,
                row_number=row_number,
                row_data=row_dict,
                raw_row=raw_row,
                iccid=iccid,
                customernumber=customernumber,
                companyid=companyid,
            )

        return RowValidationResult(
            is_valid=True,
            reason=None,
            row_number=row_number,
            row_data=row_dict,
            raw_row=raw_row,
            iccid=iccid,
            customernumber=customernumber,
            companyid=companyid,
        )


class RowDeduplicator:
    """Deduplicates rows by business key pair, keeping the latest occurrence.

    Business Key Pair:
    In telecom actives data and Doris unique key configuration (UNIQUE KEY(customernumber, iccid)),
    the primary entity is identified by (CustomerNumber, ICCID).
    When duplicate pairs appear in the same batch/file, the latest row is kept
    and earlier occurrences are dropped.
    """

    @staticmethod
    def deduplicate(rows: list[dict[str, str]]) -> list[dict[str, str]]:
        """Retain latest row for each duplicate (CustomerNumber, ICCID) pair."""
        dedup_map: dict[tuple[str, str], dict[str, str]] = {}
        for row in rows:
            pair_key = (
                (row.get("CustomerNumber") or "").strip(),
                (row.get("ICCID") or "").strip(),
            )
            # Overwriting retains the latest occurrence in source order
            dedup_map[pair_key] = row
        return list(dedup_map.values())
