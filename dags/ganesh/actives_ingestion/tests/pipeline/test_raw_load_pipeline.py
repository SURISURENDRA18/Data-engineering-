from pathlib import Path

import src.pipeline.raw_load_pipeline as raw_load_pipeline_module
from src.pipeline.file_validation_pipeline import run_validate_and_reject
from src.pipeline.raw_load_pipeline import run_raw_load

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
LANDING = "mediation/landing/"
VALID_CONTENT = (FIXTURES / "valid_actives_content.csv").read_bytes()


def _land_and_validate(s3_client, fake_doris_conn, test_settings, filename: str, body: bytes):
    """Realistic setup: a file only ever reaches validated/ (and only ever
    gets a tracking row) by going through Task 1 first, exactly as the DAG
    does. Task-2-only tests must not skip that step."""
    s3_client.put_object(Bucket="crushftp", Key=f"{LANDING}{filename}", Body=body)
    run_validate_and_reject(s3_client, fake_doris_conn, test_settings)


def test_raw_load_handles_empty_validated_folder(s3_client, fake_doris_conn, test_settings, fake_stream_load):
    """validated/ can genuinely be empty (nothing passed Task 1 yet, or
    landing/ was empty) -- must return a clean zeroed summary, not raise."""
    summary = run_raw_load(s3_client, fake_doris_conn, test_settings, stream_load_fn=fake_stream_load)

    assert summary["files_discovered"] == 0
    assert summary["files_loaded"] == 0
    assert summary["files_failed"] == 0
    assert summary["total_rows_loaded"] == 0


def test_raw_load_loads_all_rows(s3_client, fake_doris_conn, test_settings, fake_stream_load):
    _land_and_validate(s3_client, fake_doris_conn, test_settings, "HOL_01_Active_000000001_20260921050000.csv", VALID_CONTENT)

    summary = run_raw_load(s3_client, fake_doris_conn, test_settings, stream_load_fn=fake_stream_load)

    assert summary["files_loaded"] == 1
    assert summary["total_rows_loaded"] == 2
    assert len(fake_doris_conn.store.raw_rows) == 2


def test_raw_load_is_idempotent_on_rerun(s3_client, fake_doris_conn, test_settings, fake_stream_load):
    _land_and_validate(s3_client, fake_doris_conn, test_settings, "HOL_01_Active_000000002_20260921050000.csv", VALID_CONTENT)

    first = run_raw_load(s3_client, fake_doris_conn, test_settings, stream_load_fn=fake_stream_load)
    second = run_raw_load(s3_client, fake_doris_conn, test_settings, stream_load_fn=fake_stream_load)

    assert first["files_loaded"] == 1
    assert second["files_loaded"] == 0
    assert second["files_skipped_already_loaded"] == 1
    assert len(fake_doris_conn.store.raw_rows) == 2  # not duplicated


def test_raw_load_isolates_bad_file_from_good_files(s3_client, fake_doris_conn, test_settings, fake_stream_load, monkeypatch):
    # Both files are well-formed and pass Task 1 -- a Task-2-only failure
    # (e.g. a transient Doris write error) is simulated for just one of
    # them via monkeypatch, since Task 1's checks are thorough enough that
    # a file reaching validated/ is already guaranteed valid CSV/schema.
    good_filename = "HOL_01_Active_000000003_20260921050000.csv"
    failing_filename = "HOL_01_Active_000000004_20260921060000.csv"
    _land_and_validate(s3_client, fake_doris_conn, test_settings, good_filename, VALID_CONTENT)
    _land_and_validate(s3_client, fake_doris_conn, test_settings, failing_filename, VALID_CONTENT.replace(b"CUST0001", b"CUST9999"))

    real_loader = raw_load_pipeline_module.load_file_to_raw_table

    def flaky_loader(conn, settings, raw_bytes, filename, stream_load_fn=None):
        if filename == failing_filename:
            raise RuntimeError("simulated Doris write failure")
        return real_loader(conn, settings, raw_bytes, filename, stream_load_fn=stream_load_fn)

    monkeypatch.setattr(raw_load_pipeline_module, "load_file_to_raw_table", flaky_loader)

    summary = run_raw_load(s3_client, fake_doris_conn, test_settings, stream_load_fn=fake_stream_load)

    assert summary["files_loaded"] == 1
    assert summary["files_failed"] == 1
    assert len(fake_doris_conn.store.raw_rows) == 2


def test_raw_load_routes_row_rejects_to_rejected_table(s3_client, fake_doris_conn, test_settings, fake_stream_load):
    content_with_bad_row = VALID_CONTENT.replace(b"CUST0001", b"")
    _land_and_validate(
        s3_client, fake_doris_conn, test_settings, "HOL_01_Active_000000005_20260921050000.csv", content_with_bad_row
    )

    summary = run_raw_load(s3_client, fake_doris_conn, test_settings, stream_load_fn=fake_stream_load)

    assert summary["total_rows_rejected"] == 1
    assert len(fake_doris_conn.store.rejected_rows) == 1
