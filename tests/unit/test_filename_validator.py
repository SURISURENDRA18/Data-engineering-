"""Unit tests for FilenameValidator."""

from actives_ingestion.validators import FilenameValidator


def test_valid_csv():
    is_valid, meta = FilenameValidator.validate("US_01_Active_00001_20260922120000.csv")
    assert is_valid is True
    assert meta is not None
    assert meta.region == "US"
    assert meta.instance == "01"
    assert meta.feed == "Active"
    assert meta.sequence_number == 1
    assert meta.timestamp_str == "20260922120000"
    assert meta.is_gzipped is False


def test_valid_csv_gz():
    is_valid, meta = FilenameValidator.validate("EU_02_Actives_00045_20260922143000.csv.gz")
    assert is_valid is True
    assert meta is not None
    assert meta.region == "EU"
    assert meta.instance == "02"
    assert meta.feed == "Actives"
    assert meta.sequence_number == 45
    assert meta.timestamp_str == "20260922143000"
    assert meta.is_gzipped is True


def test_invalid_region():
    is_valid, _ = FilenameValidator.validate("US123_01_Active_00001_20260922120000.csv")
    assert is_valid is False

    is_valid, _ = FilenameValidator.validate("_01_Active_00001_20260922120000.csv")
    assert is_valid is False


def test_invalid_instance():
    is_valid, _ = FilenameValidator.validate("US_INST1_Active_00001_20260922120000.csv")
    assert is_valid is False


def test_invalid_feed_name():
    is_valid, _ = FilenameValidator.validate("US_01_Inactive_00001_20260922120000.csv")
    assert is_valid is False

    is_valid, _ = FilenameValidator.validate("US_01_active_00001_20260922120000.csv")
    assert is_valid is False


def test_invalid_sequence():
    is_valid, _ = FilenameValidator.validate("US_01_Active_SEQ01_20260922120000.csv")
    assert is_valid is False


def test_invalid_timestamp():
    # Non-digit timestamp
    is_valid, _ = FilenameValidator.validate("US_01_Active_00001_2026-09-22.csv")
    assert is_valid is False

    # Short timestamp (less than 14 digits)
    is_valid, _ = FilenameValidator.validate("US_01_Active_00001_20260922.csv")
    assert is_valid is False

    # Long timestamp (more than 14 digits)
    is_valid, _ = FilenameValidator.validate("US_01_Active_00001_2026092212000000.csv")
    assert is_valid is False


def test_invalid_extension():
    is_valid, _ = FilenameValidator.validate("US_01_Active_00001_20260922120000.txt")
    assert is_valid is False

    is_valid, _ = FilenameValidator.validate("US_01_Active_00001_20260922120000.parquet")
    assert is_valid is False

    is_valid, _ = FilenameValidator.validate("US_01_Active_00001_20260922120000.csv.zip")
    assert is_valid is False
