#!/usr/bin/env python3
"""CLI script to test connection, authenticate, and query the remote Apache Airflow 3 instance.

Usage:
    python scripts/test_airflow_connection.py
"""

import os
import sys

# Ensure src is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from actives_ingestion.airflow_client import AirflowClient  # noqa: E402
from actives_ingestion.config import pipeline_config  # noqa: E402


def main():
    cfg = pipeline_config.airflow
    print("=" * 60)
    print("Remote Apache Airflow 3 Connection Diagnostic")
    print("=" * 60)
    print(f"Target Host:       {cfg.host}")
    print(f"Target Port:       {cfg.port}")
    print(f"Airflow Version:   {cfg.version}")
    print(f"Web UI URL:        {cfg.ui_url}")
    print(f"REST API URL:      {cfg.api_url}")
    print(f"Username:          {cfg.user}")
    print(f"Password:          {'*' * len(cfg.password) if cfg.password else '<unset>'}")
    print("-" * 60)

    client = AirflowClient()

    # Step 1: Raw TCP socket test
    print("Step 1: Checking TCP socket connectivity...")
    tcp_ok, tcp_msg = client.check_tcp_connectivity(timeout=4.0)
    if tcp_ok:
        print(f"  [PASS] {tcp_msg}")
    else:
        print(f"  [WARN] {tcp_msg}")
        print("         (If running in a sandbox or corporate network, verify network/VPN routes)")

    # Step 2: Health check endpoint
    print("\nStep 2: Checking Airflow 3 health endpoint (/api/v2/monitor/health)...")
    health = client.check_health()
    if health.get("reachable"):
        print(f"  [PASS] Health endpoint returned HTTP {health['status_code']}")
        print(f"         Payload: {health.get('data')}")
    else:
        print(f"  [WARN] Could not reach health endpoint: {health.get('error') or health.get('status_code')}")

    # Step 3: Authenticate via POST /auth/token
    print("\nStep 3: Authenticating via Airflow 3 token endpoint (/auth/token)...")
    auth_ok, auth_msg = client.authenticate()
    if auth_ok:
        print(f"  [PASS] {auth_msg}")
    else:
        print(f"  [FAIL] {auth_msg}")

    # Step 4: Querying Airflow 3 REST API (/api/v2/dags)
    print("\nStep 4: Querying Airflow 3 REST API (/api/v2/dags) with Bearer token...")
    dags_res = client.list_dags()
    if dags_res.get("success"):
        total = dags_res.get("total_entries")
        dags = dags_res.get("dags", [])
        print(f"  [PASS] Authenticated successfully! Total registered DAGs: {total}")
        for d in dags[:10]:
            print(f"         - {d.get('dag_id')} (paused: {d.get('is_paused')})")
        if len(dags) > 10:
            print(f"         ... and {len(dags) - 10} more")

        # Check if actives_ingestion_dag is registered
        target_dag = "actives_ingestion_dag"
        matching = [d for d in dags if d.get("dag_id") == target_dag]
        if matching:
            print(f"\n  [INFO] Target DAG '{target_dag}' is present in cluster.")
        else:
            print(f"\n  [INFO] Target DAG '{target_dag}' is not currently deployed to the remote cluster.")
            print("         (Remote Airflow pulls from git; DAG is validated locally via DagBag)")
    else:
        print(f"  [FAIL] REST API response: {dags_res.get('error') or dags_res.get('status_code')}")

    print("=" * 60)


if __name__ == "__main__":
    main()
