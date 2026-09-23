"""Unit tests for sequence validation and duplicate file detection (Checks 7 and 8)."""

import io

import pytest

from actives_ingestion.constants import (
    EXPECTED_COLUMNS,
    REASON_DUPLICATE,
    REASON_SEQUENCE_INVALID,
    STATUS_SUCCESS,
)
from actives_ingestion.repositories.file_tracking_repository import (
    FileTrackingRepository,
)
from actives_ingestion.validators import FileValidator


@pytest.fixture
def repo():
    return FileTrackingRepository(in_memory=True)


@pytest.fixture
def file_validator(repo):
    return FileValidator(tracking_repository=repo)


def make_valid_content(custom_suffix=""):
    header = ",".join(EXPECTED_COLUMNS) + "\n"
    row = ["" for _ in range(46)]
    row[0] = "100"
    row[11] = f"CUST1{custom_suffix}"
    row[20] = f"89014103211118510720{custom_suffix}"
    return (header + ",".join(row) + "\n").encode("utf-8")


def test_first_sequence(repo):
    assert repo.validate_sequence("US", 1) is True
    assert repo.validate_sequence("US", 10) is True


def test_increasing_sequence(repo):
    repo.record_status(
        file_hash="hash1",
        file_name="US_01_Active_00001_20260922120000.csv",
        region="US",
        sequence_number=1,
        status=STATUS_SUCCESS,
    )
    assert repo.validate_sequence("US", 2) is True
    assert repo.validate_sequence("US", 5) is True


def test_duplicate_sequence(repo):
    repo.record_status(
        file_hash="hash1",
        file_name="US_01_Active_00001_20260922120000.csv",
        region="US",
        sequence_number=1,
        status=STATUS_SUCCESS,
    )
    assert repo.validate_sequence("US", 1) is False


def test_out_of_order_sequence(repo):
    repo.record_status(
        file_hash="hash2",
        file_name="US_01_Active_00005_20260922120000.csv",
        region="US",
        sequence_number=5,
        status=STATUS_SUCCESS,
    )
    # Sequence smaller than latest processed sequence is out-of-order
    assert repo.validate_sequence("US", 3) is False
    assert repo.validate_sequence("US", 4) is False
    assert repo.validate_sequence("US", 5) is False
    assert repo.validate_sequence("US", 6) is True


def test_sequence_independent_per_region(repo):
    repo.record_status(
        file_hash="hash_us",
        file_name="US_01_Active_00010_20260922120000.csv",
        region="US",
        sequence_number=10,
        status=STATUS_SUCCESS,
    )
    # Region EU should not be affected by US sequences
    assert repo.validate_sequence("EU", 1) is True


def test_duplicate_file_detection(file_validator, repo):
    content = make_valid_content()
    stream1 = io.BytesIO(content)

    res1 = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream1, len(content)
    )
    assert res1.is_valid is True

    repo.record_status(
        file_hash=res1.file_hash,
        file_name="US_01_Active_00001_20260922120000.csv",
        region="US",
        sequence_number=1,
        status=STATUS_SUCCESS,
    )

    stream2 = io.BytesIO(content)
    res2 = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream2, len(content)
    )
    assert res2.is_valid is False
    assert res2.reason == REASON_DUPLICATE

    # Re-validating same content under a different filename -> still duplicate
    stream3 = io.BytesIO(content)
    res3 = file_validator.validate(
        "US_01_Active_00002_20260922130000.csv", stream3, len(content)
    )
    assert res3.is_valid is False
    assert res3.reason == REASON_DUPLICATE


def test_in_batch_duplicate_and_sequence(file_validator):
    # Test two identical files in the same batch before writing to DB
    content1 = make_valid_content("_batch")
    stream1 = io.BytesIO(content1)
    res1 = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream1, len(content1)
    )
    assert res1.is_valid is True

    # Same content in same batch -> immediately caught as duplicate
    stream2 = io.BytesIO(content1)
    res2 = file_validator.validate(
        "US_01_Active_00002_20260922120000.csv", stream2, len(content1)
    )
    assert res2.is_valid is False
    assert res2.reason == REASON_DUPLICATE

    # Out of order sequence in same batch (Seq 1 was validated, now Seq 1 arrives with different content)
    content2 = make_valid_content("_diff")
    stream3 = io.BytesIO(content2)
    res3 = file_validator.validate(
        "US_01_Active_00001_20260922120000.csv", stream3, len(content2)
    )
    assert res3.is_valid is False
    assert res3.reason == REASON_SEQUENCE_INVALID
