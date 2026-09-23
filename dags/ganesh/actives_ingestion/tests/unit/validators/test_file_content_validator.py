import gzip

import pytest

from src.validators.file_content_validator import (
    DecompressionError,
    decompress_if_gzip,
    validate_has_data_rows,
    validate_is_text,
    validate_non_empty,
)


def test_validate_non_empty_rejects_zero_size():
    outcome = validate_non_empty(0)
    assert outcome.passed is False
    assert outcome.reason == "zero_size"


def test_validate_non_empty_accepts_positive_size():
    assert validate_non_empty(123).passed is True


def test_decompress_if_gzip_passes_through_plain_csv():
    content = b"a;b\n1;2\n"
    assert decompress_if_gzip(content, "file.csv") == content


def test_decompress_if_gzip_decompresses_gz():
    content = b"a;b\n1;2\n"
    compressed = gzip.compress(content)
    assert decompress_if_gzip(compressed, "file.csv.gz") == content


def test_decompress_if_gzip_raises_on_corrupt_gzip():
    with pytest.raises(DecompressionError):
        decompress_if_gzip(b"not actually gzip data", "file.csv.gz")


def test_validate_has_data_rows_rejects_header_only():
    outcome = validate_has_data_rows(b"CompanyID;CompanyName\n")
    assert outcome.passed is False
    assert outcome.reason == "header_only"


def test_validate_has_data_rows_accepts_header_plus_row():
    outcome = validate_has_data_rows(b"CompanyID;CompanyName\n1;Acme\n")
    assert outcome.passed is True


def test_validate_is_text_accepts_utf8():
    assert validate_is_text("CompanyID;CompanyName\n1;Acme\n".encode("utf-8")).passed is True


def test_validate_is_text_rejects_binary_garbage():
    binary_content = bytes(range(0, 40)) * 20  # lots of non-printable control bytes
    outcome = validate_is_text(binary_content)
    assert outcome.passed is False
    assert outcome.reason == "not_text_format"
