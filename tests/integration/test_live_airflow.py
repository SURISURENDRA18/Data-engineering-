"""Live Airflow 3 integration and end-to-end execution tests.

Connects to:
- Remote Apache Airflow 3 instance: 34.130.209.7:32002
- MinIO Object Storage: 34.130.209.7:32010 (bucket: crushftp)
- Apache Doris: 34.130.209.7:31545 (MySQL) and 32713 (Stream Load)

Verifies:
1. TCP connectivity and Airflow 3 health endpoint (/api/v2/monitor/health)
2. JWT token authentication (/auth/token)
3. Target DAG registration and configuration (surendra_actives_ingestion)
4. Full live execution: staging test files to landing -> triggering DAG run via REST API ->
   monitoring task states -> verifying validated file routing -> verifying Doris table ingestion ->
   verifying task execution logs.
5. Idempotent re-run behavior on the live cluster.
"""

import random
import socket
import string
from datetime import datetime, timezone
from typing import Any

import pymysql
import pytest

from actives_ingestion.airflow_client import AirflowClient
from actives_ingestion.config import PipelineConfig, pipeline_config
from actives_ingestion.constants import (
    EXPECTED_COLUMNS,
    ROW_REASON_NULL_OR_EMPTY_ICCID,
    STATUS_SUCCESS,
)
from actives_ingestion.minio_client import MinioService


def check_tcp_endpoint(host: str, port: int, timeout: float = 3.0) -> tuple[bool, str]:
    """Check raw TCP connectivity to a host:port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.close()
        return True, "OPEN"
    except Exception as e:
        s.close()
        return False, str(e)


def is_live_airflow_available() -> bool:
    """Check if the remote Airflow 3 host and port are reachable."""
    cfg = pipeline_config.airflow
    ok, _ = check_tcp_endpoint(cfg.host, cfg.port, timeout=3.0)
    return ok


def make_csv_data(rows_data: list[dict[str, Any]], delimiter: str = ",") -> bytes:
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


@pytest.fixture(scope="module")
def airflow_client() -> AirflowClient:
    """Fixture providing authenticated AirflowClient against live cluster."""
    if not is_live_airflow_available():
        pytest.skip(
            f"Remote Airflow 3 is unreachable at {pipeline_config.airflow.host}:{pipeline_config.airflow.port}. "
            "Skipping live Airflow test."
        )
    client = AirflowClient()
    ok, msg = client.authenticate()
    if not ok:
        pytest.skip(f"Failed to authenticate against live Airflow: {msg}")
    return client


def test_live_airflow_tcp_connectivity():
    """Verify raw TCP connectivity to remote Airflow 3."""
    cfg = pipeline_config.airflow
    ok, msg = check_tcp_endpoint(cfg.host, cfg.port)
    if not ok:
        pytest.skip(f"Live Airflow endpoint {cfg.host}:{cfg.port} unreachable: {msg}")
    assert ok is True


def test_live_airflow_health_endpoint(airflow_client: AirflowClient):
    """Verify Airflow 3 cluster health via /api/v2/monitor/health."""
    health = airflow_client.check_health()
    assert health["reachable"] is True, f"Airflow health check failed: {health.get('error')}"
    assert health["status_code"] == 200

    data = health.get("data", {})
    if isinstance(data, dict):
        assert data.get("metadatabase", {}).get("status") == "healthy"
        assert data.get("scheduler", {}).get("status") == "healthy"


def test_live_airflow_dag_registered_and_unpaused(airflow_client: AirflowClient):
    """Verify DAG surendra_actives_ingestion is registered, active, and has no import errors."""
    dag_id = "surendra_actives_ingestion"
    res = airflow_client.get_dag(dag_id)
    assert res.get("success") is True, f"Failed to get DAG '{dag_id}': {res.get('error')}"

    data = res.get("data", {})
    assert data.get("dag_id") == dag_id
    assert data.get("is_paused") is False, f"DAG '{dag_id}' is paused on the remote cluster"
    assert data.get("has_import_errors") is False, f"DAG '{dag_id}' has import errors on the remote cluster"


def test_live_airflow_end_to_end_execution(airflow_client: AirflowClient):
    """End-to-end live test: Stage feed -> Trigger Airflow DAG -> Monitor completion -> Verify Doris state."""
    config = PipelineConfig()
    minio = MinioService(config.minio)

    # 1. Generate unique session and file identifiers adhering to Check 1 regex

    tag = "".join(random.choices(string.ascii_uppercase, k=6))
    region = f"TESTAF{tag}"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    file_name = f"{region}_01_Active_00001_{ts}.csv"

    # 2. Stage test fixture in MinIO landing
    cust_1 = f"CUST_AF_{tag}_1"
    cust_2 = f"CUST_AF_{tag}_2"
    iccid_1 = f"890141{random.randint(100000000000, 999999999999)}"

    valid_rows = [
        {"CompanyID": "100", "CustomerNumber": cust_1, "ICCID": iccid_1, "PlanName": "InitialPlan"},
        {"CompanyID": "100", "CustomerNumber": cust_1, "ICCID": iccid_1, "PlanName": "UpdatedPlan"},  # Duplicate pair: latest wins
        {"CompanyID": "200", "CustomerNumber": cust_2, "ICCID": "", "PlanName": "InvalidPlan"},  # Malformed: empty ICCID
    ]
    csv_bytes = make_csv_data(valid_rows)
    landing_path = f"mediation/landing/{file_name}"
    minio.put_object_data(landing_path, csv_bytes)

    # Verify landing file was uploaded
    assert minio.object_exists(landing_path) is True

    # 3. Trigger DAG run with feed_filter configuration
    dag_id = "surendra_actives_ingestion"
    trigger_res = airflow_client.trigger_dag(dag_id, conf={"feed_filter": region})
    assert trigger_res.get("success") is True, f"Failed to trigger DAG: {trigger_res.get('error')}"

    run_id = trigger_res.get("data", {}).get("dag_run_id")
    assert run_id is not None

    # 4. Wait for DAG run completion
    wait_res = airflow_client.wait_for_dag_run(dag_id, run_id, timeout=90.0, poll_interval=2.0)
    assert wait_res.get("success") is True, f"DAG execution timed out or failed: {wait_res}"

    dag_state = wait_res.get("data", {}).get("state")
    assert dag_state == "success", f"DAG run ended in unexpected state: {dag_state}"

    # 5. Verify task instance states
    ti_res = airflow_client.get_task_instances(dag_id, run_id)
    assert ti_res.get("success") is True

    tis = {ti.get("task_id"): ti for ti in ti_res.get("data", {}).get("task_instances", [])}
    assert "validate_and_reject" in tis
    assert "load_raw_table" in tis
    assert tis["validate_and_reject"]["state"] == "success"
    assert tis["load_raw_table"]["state"] == "success"

    # 6. Verify task execution logs are retrievable
    log_events = airflow_client.get_task_log_events(dag_id, run_id, "validate_and_reject")
    assert len(log_events) > 0, "No log events found for validate_and_reject task"

    # 7. Verify MinIO: file moved to validated/ and original preserved in landing/
    validated_path = f"mediation/surendra/validated/{file_name}"
    assert minio.object_exists(validated_path) is True
    assert minio.object_exists(landing_path) is True

    # 8. Verify Doris tables if Doris is reachable
    try:
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
            # Check raw_actives_daily: deduplicated row kept latest plan
            cur.execute(
                f"SELECT * FROM {config.doris.database}.{config.doris.raw_table} WHERE file_name = %s;",
                (file_name,),
            )
            raw_rows = cur.fetchall()
            assert len(raw_rows) == 1
            assert raw_rows[0]["customernumber"] == cust_1
            assert raw_rows[0]["iccid"] == iccid_1
            assert raw_rows[0]["planname"] == "UpdatedPlan"

            # Check rejected rows: 1 malformed row
            cur.execute(
                f"SELECT * FROM {config.doris.database}.{config.doris.rejected_table} WHERE file_name = %s;",
                (file_name,),
            )
            rej_rows = cur.fetchall()
            assert len(rej_rows) == 1
            assert rej_rows[0]["reason"] == ROW_REASON_NULL_OR_EMPTY_ICCID
            assert rej_rows[0]["customernumber"] == cust_2

            # Check tracking table: status SUCCESS, rows_loaded=1, rows_rejected=1
            cur.execute(
                f"SELECT * FROM {config.doris.database}.{config.doris.tracking_table} WHERE file_name = %s;",
                (file_name,),
            )
            tracking = cur.fetchone()
            assert tracking is not None
            assert tracking["status"] == STATUS_SUCCESS
            assert tracking["rows_loaded"] == 1
            assert tracking["rows_rejected"] == 1

        conn.close()
    except Exception as e:
        pytest.fail(f"Doris verification query failed: {e}")

    # 9. Idempotent re-trigger test: re-run should skip already-processed file
    retrigger_res = airflow_client.trigger_dag(dag_id, conf={"feed_filter": region})
    assert retrigger_res.get("success") is True
    rerun_id = retrigger_res.get("data", {}).get("dag_run_id")

    rerun_wait = airflow_client.wait_for_dag_run(dag_id, rerun_id, timeout=90.0, poll_interval=2.0)
    assert rerun_wait.get("success") is True
    assert rerun_wait.get("data", {}).get("state") == "success"

    # Doris row count must remain exactly 1 (no duplicate rows inserted)
    conn2 = pymysql.connect(
        host=config.doris.host,
        port=config.doris.port,
        user=config.doris.user,
        password=config.doris.password,
        database=config.doris.database,
        connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,
    )
    with conn2.cursor() as cur:
        cur.execute(
            f"SELECT count(*) as cnt FROM {config.doris.database}.{config.doris.raw_table} WHERE file_name = %s;",
            (file_name,),
        )
        assert cur.fetchone()["cnt"] == 1
    conn2.close()
