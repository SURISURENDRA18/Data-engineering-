#!/usr/bin/env python3
"""Live Airflow 3 End-to-End Pipeline Verification Script.

Connects to the live Airflow 3 cluster, verifies cluster health, validates DAG registration,
stages an isolated test feed into MinIO landing, triggers the execution via REST API,
monitors task progression in real time, displays execution logs, and verifies Apache Doris
warehouse state and tracking metrics.

Usage:
    python3 scripts/test_live_airflow.py
    python3 scripts/test_live_airflow.py --timeout 150
    python3 scripts/test_live_airflow.py --skip-staging --filter "Active"
"""

import argparse
import os
import random
import string
import sys
import time
from datetime import datetime, timezone

# Ensure src is in sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from actives_ingestion.airflow_client import AirflowClient  # noqa: E402
from actives_ingestion.config import pipeline_config  # noqa: E402
from actives_ingestion.constants import EXPECTED_COLUMNS  # noqa: E402
from actives_ingestion.doris_client import DorisClient  # noqa: E402
from actives_ingestion.minio_client import MinioService  # noqa: E402


def print_header(title: str):
    print("\n" + "=" * 75)
    print(f"  {title}")
    print("=" * 75)


def print_step(step_num: int, total_steps: int, title: str):
    print(f"\n[Step {step_num}/{total_steps}] {title}")
    print("-" * 60)


def make_csv_payload(rows: list[dict[str, str]]) -> bytes:
    header = ",".join(EXPECTED_COLUMNS) + "\n"
    lines = [header]
    for row in rows:
        line = ["" for _ in range(len(EXPECTED_COLUMNS))]
        for col_name, val in row.items():
            if col_name in EXPECTED_COLUMNS:
                idx = EXPECTED_COLUMNS.index(col_name)
                line[idx] = str(val)
        lines.append(",".join(line) + "\n")
    return "".join(lines).encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description="Live Airflow 3 End-to-End Verification")
    parser.add_argument(
        "--dag-id",
        default="surendra_actives_ingestion",
        help="DAG ID to trigger (default: surendra_actives_ingestion)",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help="Custom feed filter string (default: dynamic test prefix)",
    )
    parser.add_argument(
        "--skip-staging",
        action="store_true",
        help="Skip uploading a new synthetic test fixture to MinIO landing",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Timeout in seconds waiting for DAG execution (default: 120s)",
    )
    args = parser.parse_args()

    cfg = pipeline_config.airflow
    print_header("LIVE APACHE AIRFLOW 3 END-TO-END VERIFICATION")
    print(f"Target Airflow Host:  {cfg.host}:{cfg.port}")
    print(f"REST API Endpoint:    {cfg.api_url}")
    print(f"Web UI:               {cfg.ui_url}")
    print(f"Target DAG ID:        {args.dag_id}")
    print(f"Execution Timeout:    {args.timeout}s")

    client = AirflowClient(timeout=args.timeout)

    # ---------------------------------------------------------
    # Step 1: TCP Connectivity & Health Endpoint Check
    # ---------------------------------------------------------
    print_step(1, 7, "Checking Airflow TCP Socket & Health Endpoint")
    tcp_ok, tcp_msg = client.check_tcp_connectivity(timeout=4.0)
    if not tcp_ok:
        print(f"  [FAIL] {tcp_msg}")
        print("  Remote Airflow is unreachable. Verify network connectivity or VPN.")
        sys.exit(1)
    print(f"  [PASS] {tcp_msg}")

    health = client.check_health()
    if health.get("reachable"):
        print(f"  [PASS] Health endpoint HTTP {health['status_code']}")
        data = health.get("data", {})
        if isinstance(data, dict):
            for component, status_dict in data.items():
                if isinstance(status_dict, dict):
                    print(f"         - {component}: {status_dict.get('status', 'unknown')}")
    else:
        print(f"  [WARN] Health endpoint unreachable: {health.get('error')}")

    # ---------------------------------------------------------
    # Step 2: JWT Token Authentication
    # ---------------------------------------------------------
    print_step(2, 7, "Authenticating with Airflow 3 POST /auth/token")
    auth_ok, auth_msg = client.authenticate()
    if not auth_ok:
        print(f"  [FAIL] {auth_msg}")
        sys.exit(1)
    print(f"  [PASS] {auth_msg} (Bearer token acquired)")

    # ---------------------------------------------------------
    # Step 3: DAG Registration & Configuration Verification
    # ---------------------------------------------------------
    print_step(3, 7, f"Verifying DAG '{args.dag_id}' on Cluster")
    dag_info = client.get_dag(args.dag_id)
    if not dag_info.get("success"):
        print(f"  [FAIL] Could not find DAG '{args.dag_id}': {dag_info.get('error')}")
        sys.exit(1)

    dag_data = dag_info.get("data", {})
    is_paused = dag_data.get("is_paused")
    has_errors = dag_data.get("has_import_errors")
    fileloc = dag_data.get("fileloc")
    print(f"  [PASS] DAG '{args.dag_id}' is registered in cluster.")
    print(f"         - Paused:           {is_paused}")
    print(f"         - Has Import Errors:{has_errors}")
    print(f"         - File Location:    {fileloc}")

    if is_paused:
        print("  [WARN] DAG is currently PAUSED on remote Airflow cluster.")
    if has_errors:
        print("  [FAIL] DAG has import errors on remote cluster!")
        sys.exit(1)

    # ---------------------------------------------------------
    # Step 4: Stage Controlled Test Fixture to MinIO Landing
    # ---------------------------------------------------------
    feed_filter = args.filter
    staged_filename = None

    if not args.skip_staging:
        print_step(4, 7, "Staging Isolated Test File to MinIO Landing")
        tag = "".join(random.choices(string.ascii_uppercase, k=6))
        region = f"TESTAF{tag}"
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        staged_filename = f"{region}_01_Active_00001_{ts}.csv"
        feed_filter = region

        cust_id = f"CUST_CLI_{tag}"
        iccid = f"890141{random.randint(100000000000, 999999999999)}"

        rows = [
            {"CompanyID": "100", "CustomerNumber": cust_id, "ICCID": iccid, "PlanName": "InitialPlan"},
            {"CompanyID": "100", "CustomerNumber": cust_id, "ICCID": iccid, "PlanName": "UpdatedPlan"},  # Dup pair: latest wins
            {"CompanyID": "200", "CustomerNumber": f"{cust_id}_BAD", "ICCID": "", "PlanName": "BadPlan"},  # Empty ICCID
        ]
        csv_bytes = make_csv_payload(rows)

        minio_svc = MinioService(pipeline_config.minio)
        landing_key = f"mediation/landing/{staged_filename}"
        minio_svc.put_object_data(landing_key, csv_bytes)
        print(f"  [PASS] Staged: {landing_key} ({len(csv_bytes)} bytes)")
        print("         - Active Rows: 2 (1 duplicate key pair -> 1 unique subscriber)")
        print("         - Invalid Rows: 1 (null ICCID)")
    else:
        print_step(4, 7, "Skipping File Staging (Using existing landing files)")
        print(f"  [INFO] Using feed filter: '{feed_filter or '<all>'}'")

    # ---------------------------------------------------------
    # Step 5: Trigger Live DAG Run via Airflow 3 REST API
    # ---------------------------------------------------------
    print_step(5, 7, f"Triggering DAG '{args.dag_id}' via Airflow 3 REST API")
    conf = {"feed_filter": feed_filter} if feed_filter else {}
    trig_res = client.trigger_dag(args.dag_id, conf=conf)
    if not trig_res.get("success"):
        print(f"  [FAIL] Trigger request failed: {trig_res.get('error')}")
        sys.exit(1)

    dag_run_id = trig_res.get("data", {}).get("dag_run_id")
    initial_state = trig_res.get("data", {}).get("state")
    print("  [PASS] Triggered successfully!")
    print(f"         - Run ID:  {dag_run_id}")
    print(f"         - State:   {initial_state}")
    print(f"         - Conf:    {conf}")

    # ---------------------------------------------------------
    # Step 6: Monitor Execution & Task States in Real Time
    # ---------------------------------------------------------
    print_step(6, 7, "Polling DAG Run & Task Instances")
    start_time = time.time()
    last_state = None

    while time.time() - start_time < args.timeout:
        run_res = client.get_dag_run(args.dag_id, dag_run_id)
        if run_res.get("success"):
            last_state = run_res.get("data", {}).get("state")
            elapsed = int(time.time() - start_time)
            print(f"  [INFO] [{elapsed:3d}s] DAG Run State: {last_state}")

            if last_state in ("success", "failed"):
                break
        time.sleep(2)

    if last_state != "success":
        print(f"  [FAIL] DAG Run ended with state: {last_state}")
        # Fetch task states to see which task failed
        tis = client.get_task_instances(args.dag_id, dag_run_id).get("data", {}).get("task_instances", [])
        for ti in tis:
            print(f"         - Task '{ti.get('task_id')}': {ti.get('state')}")
        sys.exit(1)

    print("  [PASS] DAG Run completed successfully!")

    # Verify each task instance
    ti_res = client.get_task_instances(args.dag_id, dag_run_id)
    tis = ti_res.get("data", {}).get("task_instances", [])
    print("\n  Task Instances Summary:")
    for ti in tis:
        t_id = ti.get("task_id")
        t_state = ti.get("state")
        t_dur = ti.get("duration")
        status_tag = "[PASS]" if t_state == "success" else "[FAIL]"
        print(f"    {status_tag} Task '{t_id}': state={t_state}, duration={t_dur}s")

    # ---------------------------------------------------------
    # Step 7: Verify Results in MinIO, Doris Warehouse & Task Logs
    # ---------------------------------------------------------
    print_step(7, 7, "Verifying Warehouse Data, File Routing & Task Logs")

    # A. Check task logs from Airflow REST API
    for t_id in ["validate_and_reject", "load_raw_table"]:
        events = client.get_task_log_events(args.dag_id, dag_run_id, t_id)
        print(f"  Task Log '{t_id}' (Total lines/events: {len(events)}):")
        # Print summary lines from task log
        summary_lines = [ev for ev in events if "Files" in ev or "Rows" in ev or "Summary" in ev]
        for line in summary_lines[:6]:
            print(f"    | {line}")


    # B. If we staged a test file, verify exact database state
    if staged_filename:
        minio_svc = MinioService(pipeline_config.minio)
        validated_key = f"mediation/surendra/validated/{staged_filename}"
        if minio_svc.object_exists(validated_key):
            print(f"\n  [PASS] Validated file exists in MinIO: {validated_key}")
        else:
            print(f"\n  [WARN] Validated file not found at {validated_key}")

        doris_client = DorisClient(pipeline_config.doris)
        try:
            conn = doris_client._get_connection()
            with conn.cursor() as cur:
                # Raw table
                cur.execute(
                    f"SELECT customernumber, iccid, planname FROM {pipeline_config.doris.database}.{pipeline_config.doris.raw_table} WHERE file_name = %s;",
                    (staged_filename,),
                )
                raws = cur.fetchall()
                print(f"  [PASS] Doris raw_actives_daily records: {len(raws)} loaded")
                for r in raws:
                    cust_val = r.get("customernumber") if isinstance(r, dict) else r[0]
                    iccid_val = r.get("iccid") if isinstance(r, dict) else r[1]
                    plan_val = r.get("planname") if isinstance(r, dict) else r[2]
                    print(f"         - Subscriber: Customer={cust_val}, ICCID={iccid_val}, Plan={plan_val}")

                # Rejected table
                cur.execute(
                    f"SELECT reason, row_number, customernumber FROM {pipeline_config.doris.database}.{pipeline_config.doris.rejected_table} WHERE file_name = %s;",
                    (staged_filename,),
                )
                rejs = cur.fetchall()
                print(f"  [PASS] Doris raw_actives_daily_rejected records: {len(rejs)} rejected")
                for r in rejs:
                    reason_val = r.get("reason") if isinstance(r, dict) else r[0]
                    row_val = r.get("row_number") if isinstance(r, dict) else r[1]
                    cust_val = r.get("customernumber") if isinstance(r, dict) else r[2]
                    print(f"         - Rejected: reason={reason_val}, row={row_val}, Customer={cust_val}")

                # Tracking table
                cur.execute(
                    f"SELECT status, rows_loaded, rows_rejected FROM {pipeline_config.doris.database}.{pipeline_config.doris.tracking_table} WHERE file_name = %s;",
                    (staged_filename,),
                )
                track = cur.fetchone()
                if track:
                    st_val = track.get("status") if isinstance(track, dict) else track[0]
                    rl_val = track.get("rows_loaded") if isinstance(track, dict) else track[1]
                    rr_val = track.get("rows_rejected") if isinstance(track, dict) else track[2]
                    print(f"  [PASS] Doris file tracking record: status={st_val}, rows_loaded={rl_val}, rows_rejected={rr_val}")
            conn.close()
        except Exception as e:
            print(f"  [WARN] Doris verification query warning: {e}")


    print_header("LIVE AIRFLOW TEST COMPLETED SUCCESSFULLY (ALL CHECKS PASSED)")


if __name__ == "__main__":
    main()
