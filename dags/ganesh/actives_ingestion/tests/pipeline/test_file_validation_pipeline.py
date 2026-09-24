from pathlib import Path

from src.clients.minio_client import object_exists
from src.pipeline.file_validation_pipeline import run_validate_and_reject
from src.tracking.file_tracker import get_tracking_row

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
LANDING = "mediation/landing/"
VALID_CONTENT = (FIXTURES / "valid_actives_content.csv").read_bytes()
HEADER_ONLY_CONTENT = (FIXTURES / "header_only_content.csv").read_bytes()
INVALID_HEADER_CONTENT = (FIXTURES / "invalid_header_content.csv").read_bytes()
TOO_MANY_FIELDS_CONTENT = (FIXTURES / "too_many_fields_content.csv").read_bytes()
SHORT_ROW_CONTENT = (FIXTURES / "short_row_content.csv").read_bytes()


def _put(s3_client, filename: str, body: bytes):
    s3_client.put_object(Bucket="crushftp", Key=f"{LANDING}{filename}", Body=body)


def test_empty_landing_does_not_break_the_pipeline(s3_client, fake_doris_conn, test_settings):
    """landing/ is a shared, externally-populated zone that can genuinely
    be empty at evaluation time (e.g. another candidate's pipeline having
    just drained it) -- the pipeline must return a clean zeroed summary,
    not raise."""
    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary == {"files_discovered": 0, "files_in_scope": 0, "files_validated": 0, "files_rejected": 0}


def test_valid_file_moves_to_validated_and_gets_tracked(s3_client, fake_doris_conn, test_settings):
    filename = "HOL_01_Active_000000001_20260921050000.csv"
    _put(s3_client, filename, VALID_CONTENT)

    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["files_validated"] == 1
    assert summary["files_rejected"] == 0
    assert object_exists(s3_client, "crushftp", f"mediation/validated/{filename}") is True
    # landing/ is a shared pool other candidates' pipelines also read from --
    # routing must be non-destructive, so the original stays put.
    assert object_exists(s3_client, "crushftp", f"{LANDING}{filename}") is True


def test_pattern_mismatch_file_is_rejected_without_download(s3_client, fake_doris_conn, test_settings):
    # Contains "Active" (so it's in scope) but doesn't match the full pattern.
    _put(s3_client, "BadName_Active_123.csv", VALID_CONTENT)

    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["files_rejected"] == 1
    assert summary["rejected_pattern_mismatch"] == 1
    assert object_exists(s3_client, "crushftp", "mediation/rejected/pattern_mismatch/BadName_Active_123.csv") is True


def test_files_without_active_in_name_are_ignored_entirely(s3_client, fake_doris_conn, test_settings):
    """landing/ holds other candidates'/feeds' files too -- anything without
    "Active" in the name must never be read, copied, or rejected."""
    _put(s3_client, "SNG_01_Tolling_000000001_20260921000000.csv", b"not our concern")

    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["files_in_scope"] == 0
    assert summary["files_validated"] == 0
    assert summary["files_rejected"] == 0
    assert object_exists(s3_client, "crushftp", "mediation/rejected/pattern_mismatch/SNG_01_Tolling_000000001_20260921000000.csv") is False
    assert object_exists(s3_client, "crushftp", "mediation/validated/SNG_01_Tolling_000000001_20260921000000.csv") is False


def test_zero_size_file_is_rejected(s3_client, fake_doris_conn, test_settings):
    filename = "HOL_01_Active_000000002_20260921050000.csv"
    _put(s3_client, filename, b"")

    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["rejected_zero_size"] == 1
    assert object_exists(s3_client, "crushftp", f"mediation/rejected/zero_size/{filename}") is True


def test_header_only_file_is_rejected(s3_client, fake_doris_conn, test_settings):
    filename = "HOL_01_Active_000000003_20260921050000.csv"
    _put(s3_client, filename, HEADER_ONLY_CONTENT)

    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["rejected_header_only"] == 1
    assert object_exists(s3_client, "crushftp", f"mediation/rejected/header_only/{filename}") is True


def test_invalid_header_is_rejected(s3_client, fake_doris_conn, test_settings):
    filename = "HOL_01_Active_000000004_20260921050000.csv"
    _put(s3_client, filename, INVALID_HEADER_CONTENT)

    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["rejected_invalid_column_count"] == 1
    assert object_exists(s3_client, "crushftp", f"mediation/rejected/invalid_column_count/{filename}") is True


def test_row_with_too_many_fields_is_rejected(s3_client, fake_doris_conn, test_settings):
    filename = "HOL_01_Active_000000005_20260921050000.csv"
    _put(s3_client, filename, TOO_MANY_FIELDS_CONTENT)

    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["rejected_invalid_column_count"] == 1


def test_row_with_missing_trailing_fields_is_accepted(s3_client, fake_doris_conn, test_settings):
    """Real files here routinely omit trailing optional columns entirely --
    a missing trailing field is implicitly empty, not a rejection."""
    filename = "HOL_01_Active_000000011_20260921050000.csv"
    _put(s3_client, filename, SHORT_ROW_CONTENT)

    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["files_validated"] == 1
    assert summary["files_rejected"] == 0
    assert object_exists(s3_client, "crushftp", f"mediation/validated/{filename}") is True


def test_duplicate_content_is_rejected_on_second_run(s3_client, fake_doris_conn, test_settings):
    first = "HOL_01_Active_000000006_20260921050000.csv"
    _put(s3_client, first, VALID_CONTENT)
    run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    # Same bytes, different filename/sequence -- still a content duplicate.
    second = "HOL_01_Active_000000007_20260921060000.csv"
    _put(s3_client, second, VALID_CONTENT)
    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["rejected_duplicate"] == 1
    assert object_exists(s3_client, "crushftp", f"mediation/rejected/duplicate/{second}") is True


def test_non_increasing_sequence_is_rejected(s3_client, fake_doris_conn, test_settings):
    first = "HOL_01_Active_000000008_20260921050000.csv"
    _put(s3_client, first, VALID_CONTENT)
    run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    # Different content (so it isn't a duplicate), same region, same sequence number.
    different_content = VALID_CONTENT.replace(b"CUST0001", b"CUST9999")
    second = "HOL_01_Active_000000008_20260922050000.csv"
    _put(s3_client, second, different_content)
    summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert summary["rejected_sequence_invalid"] == 1
    assert object_exists(s3_client, "crushftp", f"mediation/rejected/sequence_invalid/{second}") is True


def test_rerun_does_not_reprocess_already_routed_files(s3_client, fake_doris_conn, test_settings):
    filename = "HOL_01_Active_000000009_20260921050000.csv"
    _put(s3_client, filename, VALID_CONTENT)

    first_summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)
    second_summary = run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    assert first_summary["files_validated"] == 1
    # Still physically in landing/ (never deleted), but already routed, so
    # the second run must not re-validate or re-copy it.
    assert second_summary["files_discovered"] == 1
    assert second_summary["files_in_scope"] == 0
    assert second_summary["files_validated"] == 0
    assert object_exists(s3_client, "crushftp", f"mediation/validated/{filename}") is True


def test_tracking_row_recorded_for_validated_file(s3_client, fake_doris_conn, test_settings):
    filename = "HOL_01_Active_000000010_20260921050000.csv"
    _put(s3_client, filename, VALID_CONTENT)
    run_validate_and_reject(s3_client, fake_doris_conn, test_settings)

    from src.utils.checksum import compute_sha256

    row = get_tracking_row(fake_doris_conn, test_settings, compute_sha256(VALID_CONTENT))
    assert row["validation_status"] == "VALIDATED"
    assert row["load_status"] == "PENDING"
