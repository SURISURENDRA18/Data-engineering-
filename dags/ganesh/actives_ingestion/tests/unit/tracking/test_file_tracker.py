from src.config.actives_schema import LoadStatus, ValidationStatus
from src.models.validation_result import FileValidationReport, ValidationOutcome
from src.tracking.file_tracker import (
    ensure_tracking_table_exists,
    fetch_max_sequence_per_region,
    get_load_status,
    get_tracking_row,
    record_load_result,
    record_validation_result,
)


def _validated_report(**overrides) -> FileValidationReport:
    defaults = dict(
        file_key="mediation/landing/HOL_01_Active_000000001_20260921050000.csv",
        file_name="HOL_01_Active_000000001_20260921050000.csv",
        file_size_bytes=100,
        file_checksum="checksum-1",
        region="HOL",
        sequence_number=1,
        outcome=ValidationOutcome.ok(),
        row_count=2,
        destination_key="mediation/validated/HOL_01_Active_000000001_20260921050000.csv",
    )
    defaults.update(overrides)
    return FileValidationReport(**defaults)


def test_ensure_tracking_table_exists_is_a_noop_on_fake(fake_doris_conn, test_settings):
    ensure_tracking_table_exists(fake_doris_conn, test_settings)  # must not raise


def test_get_tracking_row_returns_none_when_absent(fake_doris_conn, test_settings):
    assert get_tracking_row(fake_doris_conn, test_settings, "missing") is None


def test_record_and_fetch_validated_result(fake_doris_conn, test_settings):
    report = _validated_report()
    record_validation_result(fake_doris_conn, test_settings, report)

    row = get_tracking_row(fake_doris_conn, test_settings, "checksum-1")
    assert row["validation_status"] == ValidationStatus.VALIDATED
    assert row["load_status"] == LoadStatus.PENDING
    assert row["region"] == "HOL"
    assert row["sequence_number"] == 1


def test_record_rejected_result_sets_not_applicable_load_status(fake_doris_conn, test_settings):
    report = _validated_report(
        file_checksum="checksum-2",
        outcome=ValidationOutcome.fail("sequence_invalid", "dup seq"),
        destination_key="mediation/rejected/sequence_invalid/x.csv",
    )
    record_validation_result(fake_doris_conn, test_settings, report)

    row = get_tracking_row(fake_doris_conn, test_settings, "checksum-2")
    assert row["validation_status"] == ValidationStatus.REJECTED
    assert row["rejection_reason"] == "sequence_invalid"
    assert row["load_status"] == LoadStatus.NOT_APPLICABLE


def test_fetch_max_sequence_per_region_only_counts_validated(fake_doris_conn, test_settings):
    record_validation_result(fake_doris_conn, test_settings, _validated_report(file_checksum="c1", sequence_number=1))
    record_validation_result(fake_doris_conn, test_settings, _validated_report(file_checksum="c2", sequence_number=3))
    record_validation_result(
        fake_doris_conn,
        test_settings,
        _validated_report(
            file_checksum="c3",
            sequence_number=99,
            outcome=ValidationOutcome.fail("sequence_invalid"),
        ),
    )

    baseline = fetch_max_sequence_per_region(fake_doris_conn, test_settings)
    assert baseline == {"HOL": 3}  # the rejected sequence=99 row must not count


def test_record_load_result_preserves_validation_fields(fake_doris_conn, test_settings):
    report = _validated_report(file_checksum="checksum-3")
    record_validation_result(fake_doris_conn, test_settings, report)

    record_load_result(
        fake_doris_conn,
        test_settings,
        "checksum-3",
        load_status=LoadStatus.LOADED,
        row_count_loaded=2,
        row_count_rejected=0,
        row_count_duplicate_dropped=0,
    )

    row = get_tracking_row(fake_doris_conn, test_settings, "checksum-3")
    assert row["load_status"] == LoadStatus.LOADED
    assert row["row_count_loaded"] == 2
    # Fields not touched by record_load_result must survive the rewrite.
    assert row["validation_status"] == ValidationStatus.VALIDATED
    assert row["region"] == "HOL"
    assert row["landing_key"] == report.file_key
    assert row["current_key"] == report.destination_key
    assert get_load_status(fake_doris_conn, test_settings, "checksum-3") == LoadStatus.LOADED
