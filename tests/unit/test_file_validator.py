"""Unit tests for FileValidator (Checks 1-6)."""

import gzip
import io

import pytest

from actives_ingestion.constants import (
    EXPECTED_COLUMNS,
    REASON_HEADER_ONLY,
    REASON_INVALID_COLUMN_COUNT,
    REASON_NOT_TEXT_FORMAT,
    REASON_ZERO_SIZE,
)
from actives_ingestion.validators import FileValidator


@pytest.fixture
def file_validator():
    return FileValidator(tracking_repository=None)


@pytest.fixture
def valid_header_line():
    return ",".join(EXPECTED_COLUMNS) + "\n"


@pytest.fixture
def valid_row_line():
    row = ["" for _ in range(46)]
    row[0] = "100"
    row[11] = "CUST1"
    row[20] = "89014103211118510720"
    return ",".join(row) + "\n"


def test_zero_byte_file(file_validator):
    stream = io.BytesIO(b"")
    result = file_validator.validate("US_01_Active_00001_20260922120000.csv", stream, file_size=0)
    assert result.is_valid is False
    assert result.reason == REASON_ZERO_SIZE


def test_header_only_file(file_validator, valid_header_line):
    content = valid_header_line.encode("utf-8")
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream, file_size=len(content)
    )
    assert result.is_valid is False
    assert result.reason == REASON_HEADER_ONLY


def test_invalid_utf8_binary_file(file_validator):
    content = b"\x00\xff\xfe\x12\x34\x56\x78\x90\xaa\xbb\xcc"
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream, file_size=len(content)
    )
    assert result.is_valid is False
    assert result.reason == REASON_NOT_TEXT_FORMAT


def test_null_byte_binary_file(file_validator):
    content = b"CompanyID,CompanyName\x00,InvoiceNumber\n100,Acme,INV1\n"
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream, file_size=len(content)
    )
    assert result.is_valid is False
    assert result.reason == REASON_NOT_TEXT_FORMAT


def test_corrupted_gzip_file(file_validator):
    content = b"Not a gzip file content"
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv.gz", stream, file_size=len(content)
    )
    assert result.is_valid is False
    assert result.reason == REASON_NOT_TEXT_FORMAT


def test_mid_stream_corrupted_gzip_file(file_validator, valid_header_line, valid_row_line):
    raw_content = (valid_header_line + (valid_row_line * 50)).encode("utf-8")
    gz_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buffer, mode="wb") as f:
        f.write(raw_content)
    gz_data = gz_buffer.getvalue()
    # Truncate gzip stream to simulate mid-stream transfer error
    truncated_gz = gz_data[: len(gz_data) // 2]
    stream = io.BytesIO(truncated_gz)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv.gz", stream, file_size=len(truncated_gz)
    )
    assert result.is_valid is False
    assert result.reason == REASON_NOT_TEXT_FORMAT


def test_valid_file(file_validator, valid_header_line, valid_row_line):
    content = (valid_header_line + valid_row_line).encode("utf-8")
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream, file_size=len(content)
    )
    assert result.is_valid is True
    assert result.reason is None
    assert result.file_hash is not None


def test_valid_semicolon_delimited_file(file_validator):
    header = ";".join(EXPECTED_COLUMNS) + "\n"
    row = ["" for _ in range(46)]
    row[0] = "100"
    row[11] = "CUST1"
    row[20] = "89014103211118510720"
    row_line = ";".join(row) + "\n"
    content = (header + row_line).encode("utf-8")
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream, file_size=len(content)
    )
    assert result.is_valid is True
    assert result.reason is None
    assert result.file_hash is not None


def test_valid_gzipped_file(file_validator, valid_header_line, valid_row_line):
    raw_content = (valid_header_line + valid_row_line).encode("utf-8")
    gz_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buffer, mode="wb") as f:
        f.write(raw_content)
    gz_data = gz_buffer.getvalue()

    stream = io.BytesIO(gz_data)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv.gz", stream, file_size=len(gz_data)
    )
    assert result.is_valid is True
    assert result.reason is None


def test_incorrect_header_count(file_validator, valid_row_line):
    bad_header = "CompanyID,CompanyName,InvoiceNumber\n"
    content = (bad_header + valid_row_line).encode("utf-8")
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream, file_size=len(content)
    )
    assert result.is_valid is False
    assert result.reason == REASON_INVALID_COLUMN_COUNT


def test_incorrect_header_order(file_validator, valid_row_line):
    reordered_columns = list(EXPECTED_COLUMNS)
    reordered_columns[0], reordered_columns[1] = reordered_columns[1], reordered_columns[0]
    bad_header = ",".join(reordered_columns) + "\n"
    content = (bad_header + valid_row_line).encode("utf-8")
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream, file_size=len(content)
    )
    assert result.is_valid is False
    assert result.reason == REASON_INVALID_COLUMN_COUNT


def test_malformed_data_row(file_validator, valid_header_line):
    malformed_row = "100,Acme,INV1\n"
    content = (valid_header_line + malformed_row).encode("utf-8")
    stream = io.BytesIO(content)
    result = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream, file_size=len(content)
    )
    assert result.is_valid is False
    assert result.reason == REASON_INVALID_COLUMN_COUNT
