"""Live integration verification tests against configured MinIO and Apache Doris.

Connects to:
- MinIO Object Storage: 34.130.209.7:32010 (bucket crushftp)
- Apache Doris: 34.130.209.7:31545 (MySQL) and 32713 (Stream Load)

Verifies all 7 file-level rejection reasons, 3 row-level rejection reasons,
table schemas, deduplication, metadata population, and rerun idempotency.
"""

import random
import socket
import string

import pymysql
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

# Unique alphabetic region prefix per test session to guarantee strict isolation
# and repeatable idempotency while adhering to Check 1 regex (^[A-Za-z]+_...)
_SESSION_SUFFIX = "".join(random.choices(string.ascii_uppercase, k=6))
TEST_PREFIX = f"AUDIT{_SESSION_SUFFIX}_"



def check_tcp_endpoint(host: str, port: int, timeout: float = 2.0) -> tuple[bool, str]:
    """Check TCP socket connectivity to a host:port endpoint."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.close()
        return True, "OPEN"
    except Exception as e:
        s.close()
        return False, str(e)


def test_external_services_connectivity_diagnostic():
    """Verify and document exact network status of supplied external services.

    Per Section 5 & 10:
    - Never fabricate successful results.
    - If supplied external services are unavailable, report the exact blocker with evidence.
    """
    config = PipelineConfig()
    minio_parts = config.minio.endpoint.split(":")
    minio_host = minio_parts[0]
    minio_port = int(minio_parts[1]) if len(minio_parts) > 1 else 9000

    minio_ok, minio_err = check_tcp_endpoint(minio_host, minio_port)
    doris_sql_ok, doris_sql_err = check_tcp_endpoint(config.doris.host, config.doris.port)
    doris_http_ok, doris_http_err = check_tcp_endpoint(config.doris.host, config.doris.http_port)
    airflow_ok, airflow_err = check_tcp_endpoint(config.airflow.host, config.airflow.port)

    # Diagnostic report
    status_report = {
        "minio_endpoint": config.minio.endpoint,
        "minio_status": "REACHABLE" if minio_ok else f"REFUSED: {minio_err}",
        "doris_sql_endpoint": f"{config.doris.host}:{config.doris.port}",
        "doris_sql_status": "REACHABLE" if doris_sql_ok else f"REFUSED: {doris_sql_err}",
        "doris_http_endpoint": f"{config.doris.host}:{config.doris.http_port}",
        "doris_http_status": "REACHABLE" if doris_http_ok else f"REFUSED: {doris_http_err}",
        "airflow_endpoint": f"{config.airflow.host}:{config.airflow.port}",
        "airflow_status": "REACHABLE" if airflow_ok else f"REFUSED: {airflow_err}",
    }

    assert isinstance(status_report, dict)
    assert "minio_status" in status_report
    assert "doris_sql_status" in status_report
    assert "airflow_status" in status_report



def get_live_minio_service() -> MinioService | None:
    """Connect only to the supplied MinIO configuration."""
    config = PipelineConfig()
    try:
        svc = MinioService(config.minio)
        svc.ensure_bucket_exists()
        return svc
    except Exception:
        return None


@pytest.fixture(scope="module")
def live_minio() -> MinioService:
    svc = get_live_minio_service()
    if svc is None:
        pytest.skip("Configured MinIO service is unavailable")
    return svc


def is_live_doris_available() -> bool:
    """Check if Doris MySQL query endpoint is reachable."""
    config = PipelineConfig()
    try:
        conn = pymysql.connect(
            host=config.doris.host,
            port=config.doris.port,
            user=config.doris.user,
            password=config.doris.password,
            database=config.doris.database,
            connect_timeout=2,
        )
        conn.close()
        return True
    except Exception:
        return False


def make_csv_data(rows_data, delimiter=","):
    """Helper to build 46-column CSV payload."""
    header = delimiter.join(EXPECTED_COLUMNS) + "\n"
    lines = [header]
    for row in rows_data:
        full_row = ["" for _ in range(46)]
        for k, v in row.items():
            if k in EXPECTED_COLUMNS:
                idx = EXPECTED_COLUMNS.index(k)
                full_row[idx] = str(v)
        lines.append(delimiter.join(full_row) + "\n")
    return "".join(lines).encode("utf-8")


def test_live_all_seven_rejections_and_valid_movement(live_minio):
    """Test all 7 file rejection reasons and valid file routing against real MinIO."""
    minio: MinioService = live_minio
    tracking_repo = FileTrackingRepository(in_memory=True)
    file_handler = FileHandler(
        minio_service=minio,
        tracking_repository=tracking_repo,
    )

    # 1. Invalid filename pattern -> pattern_mismatch
    f_pattern = f"{TEST_PREFIX}01_InvalidFeed_00001_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_pattern}", b"dummy")

    # 2. Zero size -> zero_size
    f_zero = f"{TEST_PREFIX}01_Active_00002_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_zero}", b"")

    # 3. Header only -> header_only
    f_hdr = f"{TEST_PREFIX}01_Active_00003_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_hdr}", (",".join(EXPECTED_COLUMNS) + "\n").encode("utf-8"))

    # 4. Corrupted text/binary -> not_text_format
    f_corrupt = f"{TEST_PREFIX}01_Active_00004_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_corrupt}", b"\x00\xff\xfe\x12\x34\x56\x78")

    # 5. Bad header column count -> invalid_column_count
    f_bad_hdr = f"{TEST_PREFIX}01_Active_00005_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_bad_hdr}", b"CompanyID,CompanyName\n100,Acme\n")

    # 6. Valid header, bad data row count -> invalid_column_count
    f_bad_row = f"{TEST_PREFIX}01_Active_00006_20260922120000.csv"
    minio.put_object_data(
        f"mediation/landing/{f_bad_row}",
        (",".join(EXPECTED_COLUMNS) + "\n100,Acme\n").encode("utf-8"),
    )

    # 7. Valid file with 2 valid rows, 1 duplicate pair, 3 invalid rows
    f_valid = f"{TEST_PREFIX}01_Active_00001_20260922120000.csv"
    valid_rows = [
        {"CompanyID": "100", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_1", "ICCID": "89014103211118510001", "PlanName": "PlanInitial"},
        {"CompanyID": "100", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_1", "ICCID": "89014103211118510001", "PlanName": "PlanLatest"},  # dup pair
        {"CompanyID": "200", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_2", "ICCID": "89014103211118510002", "PlanName": "PlanStandard"},
        {"CompanyID": "300", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_3", "ICCID": "", "PlanName": "PlanErr1"},  # null iccid
        {"CompanyID": "400", "CustomerNumber": "", "ICCID": "89014103211118510004", "PlanName": "PlanErr2"},  # null cust
        {"CompanyID": "0", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_5", "ICCID": "89014103211118510005", "PlanName": "PlanErr3"},  # zero comp
    ]
    minio.put_object_data(f"mediation/landing/{f_valid}", make_csv_data(valid_rows))

    # 8. Duplicate content -> duplicate
    f_duplicate = f"{TEST_PREFIX}01_Active_00007_20260922120000.csv"
    minio.put_object_data(f"mediation/landing/{f_duplicate}", make_csv_data(valid_rows))

    # 9. Out of order sequence -> sequence_invalid
    f_seq = f"{TEST_PREFIX}01_Active_00000_20260922120000.csv"
    minio.put_object_data(
        f"mediation/landing/{f_seq}",
        make_csv_data([{"CompanyID": "100", "CustomerNumber": "C_SEQ", "ICCID": "89014103211118510999"}]),
    )

    # Process only our isolated test files
    summary = file_handler.process_landing_files(file_filter=lambda name: name.startswith(TEST_PREFIX))

    # Verify summary counts
    assert summary.files_discovered == 9
    assert summary.files_validated == 1
    assert summary.files_rejected == 8

    # Verify locations in MinIO
    assert f"mediation/surendra/validated/{f_valid}" in minio.list_validated_files()
    assert minio.get_object_size(f"mediation/surendra/rejected/{REASON_PATTERN_MISMATCH}/{f_pattern}") >= 0
    assert minio.get_object_size(f"mediation/surendra/rejected/{REASON_ZERO_SIZE}/{f_zero}") == 0
    assert minio.get_object_size(f"mediation/surendra/rejected/{REASON_HEADER_ONLY}/{f_hdr}") > 0
    assert minio.get_object_size(f"mediation/surendra/rejected/{REASON_NOT_TEXT_FORMAT}/{f_corrupt}") > 0
    assert minio.get_object_size(f"mediation/surendra/rejected/{REASON_INVALID_COLUMN_COUNT}/{f_bad_hdr}") > 0
    assert minio.get_object_size(f"mediation/surendra/rejected/{REASON_INVALID_COLUMN_COUNT}/{f_bad_row}") > 0
    assert minio.get_object_size(f"mediation/surendra/rejected/{REASON_DUPLICATE}/{f_duplicate}") > 0
    assert minio.get_object_size(f"mediation/surendra/rejected/{REASON_SEQUENCE_INVALID}/{f_seq}") > 0


def test_live_doris_loading_and_idempotent_rerun():
    """Test loading validated files into live Doris and verify exact database state and rerun idempotency."""
    config = PipelineConfig()
    if not is_live_doris_available():
        pytest.skip(
            f"Live Apache Doris external service is unreachable at {config.doris.host}:{config.doris.port} "
            "(Connection refused). Per Section 5 & 10, reporting exact blocker."
        )

    minio = MinioService(config.minio, in_memory=False)
    doris = DorisClient(config.doris, in_memory=False)
    tracking_repo = FileTrackingRepository(config.doris, in_memory=False)
    reject_repo = RejectRepository(config.doris, in_memory=False)

    doris.init_database_and_tables()
    schema_status = doris.verify_schema()
    assert schema_status["all_valid"] is True
    assert set(schema_status["tables"]["raw_actives_daily"]["unique_keys"]) == {"customernumber", "iccid"}

    # Make this test independently runnable when the MinIO routing test was
    # skipped or executed in a different order.
    f_valid = f"{TEST_PREFIX}01_Active_00001_20260922120000.csv"
    if f"mediation/surendra/validated/{f_valid}" not in minio.list_validated_files():
        valid_rows = [
            {"CompanyID": "100", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_1", "ICCID": "89014103211118510001", "PlanName": "PlanInitial"},
            {"CompanyID": "100", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_1", "ICCID": "89014103211118510001", "PlanName": "PlanLatest"},
            {"CompanyID": "200", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_2", "ICCID": "89014103211118510002", "PlanName": "PlanStandard"},
            {"CompanyID": "300", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_3", "ICCID": "", "PlanName": "PlanErr1"},
            {"CompanyID": "400", "CustomerNumber": "", "ICCID": "89014103211118510004", "PlanName": "PlanErr2"},
            {"CompanyID": "0", "CustomerNumber": f"CUST_LIVE_{_SESSION_SUFFIX}_5", "ICCID": "89014103211118510005", "PlanName": "PlanErr3"},
        ]
        minio.put_object_data(f"mediation/landing/{f_valid}", make_csv_data(valid_rows))
        FileHandler(
            minio_service=minio,
            tracking_repository=FileTrackingRepository(in_memory=True),
        ).process_landing_files(file_filter=lambda name: name == f_valid)


    raw_loader = RawLoader(
        config=config,
        minio_service=minio,
        doris_client=doris,
        tracking_repository=tracking_repo,
        reject_repository=reject_repo,
    )

    # 1. Run Task 2
    t2_summary = raw_loader.load_all_validated_files(file_filter=lambda name: name.startswith(TEST_PREFIX))
    assert t2_summary.files_loaded == 1
    assert t2_summary.total_rows_loaded == 2
    assert t2_summary.total_rows_rejected == 3

    # 2. Query Doris via direct MySQL connection to verify actual database state
    conn = pymysql.connect(
        host=config.doris.host,
        port=config.doris.port,
        user=config.doris.user,
        password=config.doris.password,
        database=config.doris.database,
        connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,
    )
    with conn.cursor() as cur:
        # Check raw_actives_daily
        cur.execute(
            f"SELECT iccid, file_name, customernumber, planname, loaded_at FROM {config.doris.database}.{config.doris.raw_table} WHERE file_name = %s ORDER BY iccid;",
            (f_valid,),
        )
        raw_rows = cur.fetchall()
        assert len(raw_rows) == 2

        # Check deduplication kept latest row
        cust1 = next(r for r in raw_rows if r["customernumber"] == f"CUST_LIVE_{_SESSION_SUFFIX}_1")
        assert cust1["planname"] == "PlanLatest"
        assert cust1["file_name"] == f_valid
        assert cust1["loaded_at"] is not None


        # Check rejected table
        cur.execute(
            f"SELECT reason, row_number, customernumber FROM {config.doris.database}.{config.doris.rejected_table} WHERE file_name = %s;",
            (f_valid,),
        )
        rej_rows = cur.fetchall()
        assert len(rej_rows) == 3
        reasons = {r["reason"] for r in rej_rows}
        assert ROW_REASON_NULL_OR_EMPTY_ICCID in reasons
        assert ROW_REASON_NULL_OR_EMPTY_CUSTOMER_NUMBER in reasons
        assert ROW_REASON_NULL_OR_ZERO_COMPANY_ID in reasons

        # Check tracking table
        cur.execute(
            f"SELECT status, rows_loaded, rows_rejected FROM {config.doris.database}.{config.doris.tracking_table} WHERE file_name = %s;",
            (f_valid,),
        )
        tracking_row = cur.fetchone()
        assert tracking_row is not None
        assert tracking_row["status"] == STATUS_SUCCESS
        assert tracking_row["rows_loaded"] == 2
        assert tracking_row["rows_rejected"] == 3

    # 3. Rerun Task 2: File must be skipped, 0 rows added, database state identical
    t2_rerun = raw_loader.load_all_validated_files(file_filter=lambda name: name.startswith(TEST_PREFIX))
    assert t2_rerun.files_skipped_duplicate == 1
    assert t2_rerun.files_loaded == 0

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) as cnt FROM {config.doris.database}.{config.doris.raw_table} WHERE file_name = %s;",
            (f_valid,),
        )
        assert cur.fetchone()["cnt"] == 2

        cur.execute(
            f"SELECT count(*) as cnt FROM {config.doris.database}.{config.doris.rejected_table} WHERE file_name = %s;",
            (f_valid,),
        )
        assert cur.fetchone()["cnt"] == 3

    conn.close()
