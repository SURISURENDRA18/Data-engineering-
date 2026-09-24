"""End-to-end pipeline integration tests.

Verifies:
1. Landing discovery and validation routing
2. Invalid files routed to rejected/<reason>/
3. Valid files routed to validated/
4. Task 2 row validation, deduplication, raw Doris loading, and rejected row persistence
5. Metadata fields: file_name and loaded_at populated
6. File-level idempotency on re-run (skipped)
7. Per-file isolation: malformed file failure never halts valid files
8. Failure scenario recovery: load succeeds, tracking update fails, rerun is idempotent
"""

import pytest

from actives_ingestion.config import PipelineConfig
from actives_ingestion.constants import (
    EXPECTED_COLUMNS,
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
    STATUS_SUCCESS,
)
from actives_ingestion.doris_client import DorisClient
from actives_ingestion.file_handler import FileHandler
from actives_ingestion.minio_client import MinioService
from actives_ingestion.raw_loader import RawLoader
from actives_ingestion.repositories.file_tracking_repository import (
    FileTrackingRepository,
)
from actives_ingestion.repositories.reject_repository import RejectRepository


@pytest.fixture
def pipeline_env():
    """Build in-memory pipeline infrastructure for hermetic testing."""
    config = PipelineConfig()
    minio = MinioService(config.minio, in_memory=True)
    doris = DorisClient(config.doris, in_memory=True)
    tracking_repo = FileTrackingRepository(config.doris, in_memory=True)
    reject_repo = RejectRepository(config.doris, in_memory=True)

    file_handler = FileHandler(
        config=config,
        minio_service=minio,
        tracking_repository=tracking_repo,
    )

    raw_loader = RawLoader(
        config=config,
        minio_service=minio,
        doris_client=doris,
        tracking_repository=tracking_repo,
        reject_repository=reject_repo,
    )

    return {
        "config": config,
        "minio": minio,
        "doris": doris,
        "tracking_repo": tracking_repo,
        "reject_repo": reject_repo,
        "file_handler": file_handler,
        "raw_loader": raw_loader,
    }


def make_csv_data(rows_data):
    """Helper to build 46-column CSV payload."""
    header = ",".join(EXPECTED_COLUMNS) + "\n"
    lines = [header]
    for row in rows_data:
        full_row = ["" for _ in range(46)]
        for k, v in row.items():
            if k in EXPECTED_COLUMNS:
                idx = EXPECTED_COLUMNS.index(k)
                full_row[idx] = str(v)
        lines.append(",".join(full_row) + "\n")
    return "".join(lines).encode("utf-8")


def test_full_pipeline_happy_path(pipeline_env):
    minio = pipeline_env["minio"]
    file_handler = pipeline_env["file_handler"]
    raw_loader = pipeline_env["raw_loader"]
    doris = pipeline_env["doris"]
    reject_repo = pipeline_env["reject_repo"]

    # 1. Place a file with valid rows, duplicate pair, and invalid rows in landing
    file_name = "US_01_Active_00001_20260922120000.csv"
    rows = [
        {"CompanyID": "100", "CustomerNumber": "CUST001", "ICCID": "ICCID_001", "PlanName": "Basic"},
        {"CompanyID": "100", "CustomerNumber": "CUST001", "ICCID": "ICCID_001", "PlanName": "Premium"},
        {"CompanyID": "200", "CustomerNumber": "CUST002", "ICCID": "ICCID_002", "PlanName": "Standard"},
        {"CompanyID": "300", "CustomerNumber": "CUST003", "ICCID": "", "PlanName": "Standard"},
        {"CompanyID": "400", "CustomerNumber": "", "ICCID": "ICCID_004", "PlanName": "Standard"},
        {"CompanyID": "0", "CustomerNumber": "CUST005", "ICCID": "ICCID_005", "PlanName": "Standard"},
    ]
    data = make_csv_data(rows)
    minio.put_object_data(f"mediation/landing/{file_name}", data)

    # 2. Run Task 1 (validate_and_reject)
    t1_summary = file_handler.process_landing_files()
    assert t1_summary.files_discovered == 1
    assert t1_summary.files_validated == 1
    assert t1_summary.files_rejected == 0

    # MinIO read/copy-only requirement: Original file is permanently retained in landing
    assert minio.list_landing_files() == [f"mediation/landing/{file_name}"]
    assert minio.list_validated_files() == [f"mediation/surendra/validated/{file_name}"]

    # 3. Run Task 2 (load_raw_table)
    t2_summary = raw_loader.load_all_validated_files()
    assert t2_summary.files_loaded == 1
    # 3 valid raw records -> 1 duplicate dropped -> 2 loaded
    assert t2_summary.total_rows_loaded == 2
    assert t2_summary.total_rows_rejected == 3

    # 4. Check loaded rows in Doris
    loaded = doris.get_loaded_rows()
    assert len(loaded) == 2
    # Check deduplication kept the latest row (Premium)
    cust1 = next(r for r in loaded if r["customernumber"] == "CUST001")
    assert cust1["planname"] == "Premium"
    assert cust1["file_name"] == file_name
    assert "loaded_at" in cust1 and cust1["loaded_at"] != ""

    # 5. Check rejected rows
    rejected = reject_repo.get_rejected_rows()
    assert len(rejected) == 3
    reasons = {r["reason"] for r in rejected}
    assert ROW_REASON_NULL_OR_EMPTY_ICCID in reasons
    assert ROW_REASON_NULL_OR_EMPTY_CUSTOMER_NUMBER in reasons
    assert ROW_REASON_NULL_OR_ZERO_COMPANY_ID in reasons

    # 6. Idempotency test: rerun Task 2 -> file must be skipped
    t2_rerun_summary = raw_loader.load_all_validated_files()
    assert t2_rerun_summary.files_skipped_duplicate == 1
    assert t2_rerun_summary.files_loaded == 0

    # 7. Idempotency test: rerun Task 1 -> landing file stays in landing and is safely skipped
    t1_rerun_summary = file_handler.process_landing_files()
    assert t1_rerun_summary.files_discovered == 1
    assert t1_rerun_summary.files_validated == 0
    assert t1_rerun_summary.files_rejected == 0
    assert t1_rerun_summary.files_skipped_duplicate == 1
    assert minio.list_landing_files() == [f"mediation/landing/{file_name}"]
    assert minio.list_validated_files() == [f"mediation/surendra/validated/{file_name}"]


def test_full_pipeline_gzipped_file(pipeline_env):
    """Verify that .csv.gz files are validated, routed, uncompressed, and loaded into Doris."""
    import gzip

    minio = pipeline_env["minio"]
    file_handler = pipeline_env["file_handler"]
    raw_loader = pipeline_env["raw_loader"]
    doris = pipeline_env["doris"]

    file_name = "EU_02_Actives_00001_20260922120000.csv.gz"
    rows = [
        {"CompanyID": "500", "CustomerNumber": "EU_CUST01", "ICCID": "EU_ICCID_01", "PlanName": "EU_Plan"},
    ]
    raw_csv = make_csv_data(rows)
    gz_data = gzip.compress(raw_csv)
    minio.put_object_data(f"mediation/landing/{file_name}", gz_data)

    t1_summary = file_handler.process_landing_files()
    assert t1_summary.files_validated == 1
    assert minio.list_validated_files() == [f"mediation/surendra/validated/{file_name}"]

    t2_summary = raw_loader.load_all_validated_files()
    assert t2_summary.files_loaded == 1
    assert t2_summary.total_rows_loaded == 1

    loaded = doris.get_loaded_rows()
    assert len(loaded) == 1
    assert loaded[0]["customernumber"] == "EU_CUST01"
    assert loaded[0]["file_name"] == file_name
    assert loaded[0]["loaded_at"] != ""


def test_file_isolation_and_rejection_routing(pipeline_env):
    minio = pipeline_env["minio"]
    file_handler = pipeline_env["file_handler"]
    raw_loader = pipeline_env["raw_loader"]

    good_file = "US_01_Active_00001_20260922120000.csv"
    bad_file = "US_01_Active_00002_20260922120000.csv"

    good_data = make_csv_data([
        {"CompanyID": "100", "CustomerNumber": "C1", "ICCID": "ICC1"}
    ])
    minio.put_object_data(f"mediation/landing/{good_file}", good_data)
    minio.put_object_data(f"mediation/landing/{bad_file}", b"")

    summary = file_handler.process_landing_files()
    assert summary.files_discovered == 2
    assert summary.files_validated == 1
    assert summary.files_rejected == 1

    assert minio.list_validated_files() == [f"mediation/surendra/validated/{good_file}"]
    rejected_files = minio.list_rejected_files()
    assert f"mediation/surendra/rejected/zero_size/{bad_file}" in rejected_files

    # Task 2 succeeds for the valid file despite the invalid file
    t2_summary = raw_loader.load_all_validated_files()
    assert t2_summary.files_loaded == 1
    assert t2_summary.total_rows_loaded == 1


def test_task_2_per_file_isolation(pipeline_env):
    """Verify Section 10: file 1 loads, file 2 fails, file 3 still loads."""
    minio = pipeline_env["minio"]
    raw_loader = pipeline_env["raw_loader"]
    doris = pipeline_env["doris"]

    file1 = "US_01_Active_00001_20260922120000.csv"
    file2 = "US_01_Active_00002_20260922120000.csv"
    file3 = "US_01_Active_00003_20260922120000.csv"

    data1 = make_csv_data([{"CompanyID": "100", "CustomerNumber": "C1", "ICCID": "ICC1"}])
    # file2 is corrupt binary in validated folder
    data2 = b"\x00\xff\xfe\x12\x34"
    data3 = make_csv_data([{"CompanyID": "300", "CustomerNumber": "C3", "ICCID": "ICC3"}])

    minio.put_object_data(f"mediation/surendra/validated/{file1}", data1)
    minio.put_object_data(f"mediation/surendra/validated/{file2}", data2)
    minio.put_object_data(f"mediation/surendra/validated/{file3}", data3)

    summary = raw_loader.load_all_validated_files()
    # file 1 succeeds, file 2 fails, file 3 succeeds
    assert summary.files_loaded == 2
    assert summary.files_failed == 1
    assert summary.total_rows_loaded == 2

    loaded_iccids = {r["iccid"] for r in doris.get_loaded_rows()}
    assert "ICC1" in loaded_iccids
    assert "ICC3" in loaded_iccids


def test_failure_recovery_idempotency(pipeline_env):
    """Verify Section 9 failure scenario: load succeeds, tracking update fails, rerun is safe."""
    minio = pipeline_env["minio"]
    raw_loader = pipeline_env["raw_loader"]
    doris = pipeline_env["doris"]
    reject_repo = pipeline_env["reject_repo"]
    tracking_repo = pipeline_env["tracking_repo"]

    file_name = "US_01_Active_00001_20260922120000.csv"
    data = make_csv_data([
        {"CompanyID": "100", "CustomerNumber": "C1", "ICCID": "ICC1"},
        {"CompanyID": "0", "CustomerNumber": "C2", "ICCID": "ICC2"},  # rejected
    ])
    minio.put_object_data(f"mediation/surendra/validated/{file_name}", data)

    # Simulate tracking update failure on first attempt
    orig_record_status = tracking_repo.record_status
    call_count = [0]

    def failing_record_status(*args, **kwargs):
        call_count[0] += 1
        if kwargs.get("status") == STATUS_SUCCESS and call_count[0] <= 2:
            raise RuntimeError("Simulated tracking DB timeout")
        return orig_record_status(*args, **kwargs)

    tracking_repo.record_status = failing_record_status

    # Run 1: load runs, but record_status fails
    summary1 = raw_loader.load_all_validated_files()
    assert summary1.files_failed == 1

    # Restore normal tracking
    tracking_repo.record_status = orig_record_status

    # Run 2: pipeline retried
    summary2 = raw_loader.load_all_validated_files()
    assert summary2.files_loaded == 1
    assert summary2.total_rows_loaded == 1

    # Verify no duplicate rows in raw table or rejected table
    raw_rows = [r for r in doris.get_loaded_rows() if r["file_name"] == file_name]
    assert len(raw_rows) == 1
    assert raw_rows[0]["iccid"] == "ICC1"

    rejected_rows = [r for r in reject_repo.get_rejected_rows() if r["file_name"] == file_name]
    assert len(rejected_rows) == 1
    assert rejected_rows[0]["customernumber"] == "C2"


def test_landing_files_never_deleted_and_rerun_idempotent(pipeline_env):
    """Verify hard requirement: MinIO files are never deleted and pipeline reruns are 100% idempotent."""
    minio = pipeline_env["minio"]
    file_handler = pipeline_env["file_handler"]
    raw_loader = pipeline_env["raw_loader"]
    doris = pipeline_env["doris"]

    file_valid = "US_01_Active_00001_20260922120000.csv"
    file_invalid = "US_01_Active_00002_20260922120000.csv"

    valid_data = make_csv_data([
        {"CompanyID": "100", "CustomerNumber": "CUST_IDEMP_1", "ICCID": "ICC_IDEMP_1", "PlanName": "PlanA"},
    ])
    # Put valid and zero-size invalid files into landing
    minio.put_object_data(f"mediation/landing/{file_valid}", valid_data)
    minio.put_object_data(f"mediation/landing/{file_invalid}", b"")

    # Initial Run: Task 1
    t1_run1 = file_handler.process_landing_files()
    assert t1_run1.files_discovered == 2
    assert t1_run1.files_validated == 1
    assert t1_run1.files_rejected == 1
    assert t1_run1.files_skipped_duplicate == 0

    # Verify original files are permanently kept in landing/
    landing_files = minio.list_landing_files()
    assert f"mediation/landing/{file_valid}" in landing_files
    assert f"mediation/landing/{file_invalid}" in landing_files

    # Verify destinations exist
    assert f"mediation/surendra/validated/{file_valid}" in minio.list_validated_files()
    assert f"mediation/surendra/rejected/zero_size/{file_invalid}" in minio.list_rejected_files()

    # Initial Run: Task 2
    t2_run1 = raw_loader.load_all_validated_files()
    assert t2_run1.files_loaded == 1
    assert t2_run1.total_rows_loaded == 1

    # Verify validated file is NEVER deleted from validated/
    assert f"mediation/surendra/validated/{file_valid}" in minio.list_validated_files()

    # Airflow Rerun / Retry 1: Task 1 rerun
    t1_run2 = file_handler.process_landing_files()
    assert t1_run2.files_discovered == 2
    assert t1_run2.files_validated == 0
    assert t1_run2.files_rejected == 0
    assert t1_run2.files_skipped_duplicate == 2

    # Landing files still intact
    landing_files_rerun = minio.list_landing_files()
    assert f"mediation/landing/{file_valid}" in landing_files_rerun
    assert f"mediation/landing/{file_invalid}" in landing_files_rerun

    # Airflow Rerun / Retry 2: Task 2 rerun
    t2_run2 = raw_loader.load_all_validated_files()
    assert t2_run2.files_loaded == 0
    assert t2_run2.files_skipped_duplicate == 1

    # Verify no duplicate records in Doris
    loaded_rows = [r for r in doris.get_loaded_rows() if r["file_name"] == file_valid]
    assert len(loaded_rows) == 1
    assert loaded_rows[0]["iccid"] == "ICC_IDEMP_1"


def test_all_seven_rejection_reasons_and_valid_routing_in_surendra_namespace(pipeline_env):
    """Verify that all 7 rejection checks route to mediation/surendra/rejected/<reason>/ and valid to mediation/surendra/validated/."""
    minio = pipeline_env["minio"]
    file_handler = pipeline_env["file_handler"]

    # 1. Valid file
    f_valid = "US_01_Active_00001_20260922120000.csv"
    valid_data = make_csv_data([{"CompanyID": "100", "CustomerNumber": "C1", "ICCID": "ICC1"}])
    minio.put_object_data(f"mediation/landing/{f_valid}", valid_data)

    # 2. Check 1: pattern_mismatch
    f_pattern = "Invalid_Prefix_00001_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_pattern}", valid_data)

    # 3. Check 2: zero_size
    f_zero = "US_01_Active_00002_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_zero}", b"")

    # 4. Check 3: header_only
    f_hdr = "US_01_Active_00003_20260922120000.csv"
    hdr_data = (",".join(EXPECTED_COLUMNS) + "\n").encode("utf-8")
    minio.put_object_data(f"mediation/landing/{f_hdr}", hdr_data)

    # 5. Check 4: not_text_format
    f_corrupt = "US_01_Active_00004_20260922120000.csv"
    corrupt_data = b"\x00\xff\xfe\x12\x34\x56"
    minio.put_object_data(f"mediation/landing/{f_corrupt}", corrupt_data)

    # 6. Check 5: invalid_column_count (header mismatch)
    f_bad_hdr = "US_01_Active_00005_20260922120000.csv"
    bad_hdr_data = b"col1,col2,col3\nval1,val2,val3\n"
    minio.put_object_data(f"mediation/landing/{f_bad_hdr}", bad_hdr_data)

    # 7. Check 6: invalid_column_count (data row field mismatch)
    f_bad_row = "US_01_Active_00006_20260922120000.csv"
    bad_row_data = (",".join(EXPECTED_COLUMNS) + "\nval1,val2\n").encode("utf-8")
    minio.put_object_data(f"mediation/landing/{f_bad_row}", bad_row_data)

    # 8. Check 7: duplicate
    f_dup = "US_01_Active_00007_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_dup}", valid_data)

    # 9. Check 8: sequence_invalid (sequence number 0 < 1)
    f_seq = "US_01_Active_00000_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_seq}", valid_data)

    summary = file_handler.process_landing_files()
    assert summary.files_discovered == 9
    assert summary.files_validated == 1
    assert summary.files_rejected == 8
    assert summary.files_skipped_duplicate == 0

    # Verify all 9 files are permanently retained in mediation/landing/
    landing_files = minio.list_landing_files()
    for f in [f_valid, f_pattern, f_zero, f_hdr, f_corrupt, f_bad_hdr, f_bad_row, f_dup, f_seq]:
        assert f"mediation/landing/{f}" in landing_files

    # Verify validated file is in mediation/surendra/validated/
    assert f"mediation/surendra/validated/{f_valid}" in minio.list_validated_files()

    # Verify rejected files are in mediation/surendra/rejected/<reason>/
    assert minio.object_exists(f"mediation/surendra/rejected/{REASON_PATTERN_MISMATCH}/{f_pattern}")
    assert minio.object_exists(f"mediation/surendra/rejected/{REASON_ZERO_SIZE}/{f_zero}")
    assert minio.object_exists(f"mediation/surendra/rejected/{REASON_HEADER_ONLY}/{f_hdr}")
    assert minio.object_exists(f"mediation/surendra/rejected/{REASON_NOT_TEXT_FORMAT}/{f_corrupt}")
    assert minio.object_exists(f"mediation/surendra/rejected/{REASON_INVALID_COLUMN_COUNT}/{f_bad_hdr}")
    assert minio.object_exists(f"mediation/surendra/rejected/{REASON_INVALID_COLUMN_COUNT}/{f_bad_row}")
    assert minio.object_exists(f"mediation/surendra/rejected/{REASON_DUPLICATE}/{f_dup}")
    assert minio.object_exists(f"mediation/surendra/rejected/{REASON_SEQUENCE_INVALID}/{f_seq}")

    # Verify no files were placed in old shared destinations
    old_validated = minio._list_prefix("mediation/validated/")
    old_rejected = minio._list_prefix("mediation/rejected/")
    assert len(old_validated) == 0
    assert len(old_rejected) == 0

    # Verify rerun idempotency: all 9 files skipped, 0 re-routed
    rerun_summary = file_handler.process_landing_files()
    assert rerun_summary.files_discovered == 9
    assert rerun_summary.files_validated == 0
    assert rerun_summary.files_rejected == 0
    assert rerun_summary.files_skipped_duplicate == 9
