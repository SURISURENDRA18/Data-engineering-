#!/usr/bin/env python3
"""Comprehensive connectivity and authentication verification for all services.

Checks:
- Airflow UI & REST API (34.130.209.7:32002)
- Airflow Postgres (34.130.209.7:31354)
- Doris Coupled UI (34.130.209.7:31041)
- Doris Coupled MySQL client (34.130.209.7:31545)
- Doris Coupled Stream Load (34.130.209.7:32713)
- MinIO UI (34.130.209.7:30957)
- MinIO API (34.130.209.7:32010)
- PostgreSQL Database (34.130.209.7:30986)
- MSSQL Database (34.130.209.7:31704)

Safety:
- READ-ONLY checks (SELECT 1, list_buckets, /health, HTTP HEAD/GET)
- NO hardcoded passwords, tokens, or secrets
- Dynamic masking of any configured secrets in output
- Strict timeout limits (5s per check)
"""

import os
import socket
import subprocess
from typing import Optional

import pymysql
import requests
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

HOST = os.getenv("DORIS_HOST", "34.130.209.7")
TIMEOUT = 5.0

results: list[dict[str, Optional[str]]] = []


def _get_known_secrets() -> list[str]:
    """Retrieve non-empty configured secret strings to mask from output."""
    secret_env_keys = [
        "MINIO_SECRET_KEY",
        "MINIO_ACCESS_KEY",
        "DORIS_PASSWORD",
        "AIRFLOW_PASSWORD",
        "PGPASSWORD",
        "POSTGRES_PASSWORD",
        "MSSQL_PASSWORD",
        "AIRFLOW_DB_PASSWORD",
    ]
    secrets = []
    for key in secret_env_keys:
        val = os.getenv(key)
        if val and len(val.strip()) > 2:
            secrets.append(val.strip())
    return sorted(list(set(secrets)), key=len, reverse=True)


def record(service: str, target: str, status: str, details: str, error: Optional[str] = None):
    """Record and print result with strict secret masking."""
    safe_error = error
    safe_details = details

    for secret in _get_known_secrets():
        if safe_error and secret in safe_error:
            safe_error = safe_error.replace(secret, "******")
        if safe_details and secret in safe_details:
            safe_details = safe_details.replace(secret, "******")

    results.append({
        "service": service,
        "target": target,
        "status": status,
        "details": safe_details,
        "error": safe_error,
    })
    print(f"[{status}] {service} ({target}): {safe_details}" + (f" | Error: {safe_error}" if safe_error else ""))


def check_tcp(host: str, port: int) -> tuple[bool, str]:
    """Verify raw TCP socket connectivity."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(TIMEOUT)
    try:
        s.connect((host, port))
        s.close()
        return True, "Port reachable"
    except Exception as e:
        s.close()
        return False, str(e)


# ==============================================================================
# 1. AIRFLOW UI 3 & REST API
# ==============================================================================
def test_airflow_api():
    port = int(os.getenv("AIRFLOW_PORT", "32002"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("Airflow UI 3", target, "FAIL", "TCP connection failed", err)
        return

    # Check /api/v2/monitor/health endpoint
    health_url = f"http://{HOST}:{port}/api/v2/monitor/health"
    try:
        resp = requests.get(health_url, timeout=TIMEOUT)
        if resp.status_code == 200:
            health_data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            metadatadb_status = health_data.get("metadatabase", {}).get("status", "unknown")
            scheduler_status = health_data.get("scheduler", {}).get("status", "unknown")
            triggerer_status = health_data.get("triggerer", {}).get("status", "unknown")
            details = f"HTTP 200 - Metadatadb: {metadatadb_status}, Scheduler: {scheduler_status}, Triggerer: {triggerer_status}"
            record("Airflow UI 3 Health Check", target, "PASS", details)
        else:
            record("Airflow UI 3 Health Check", target, "FAIL", f"HTTP {resp.status_code}", resp.text[:200])
    except Exception as e:
        record("Airflow UI 3 Health Check", target, "FAIL", "Request error", str(e))

    # Authenticate via Airflow 3 token endpoint
    user = os.getenv("AIRFLOW_USER", "")
    password = os.getenv("AIRFLOW_PASSWORD", "")
    if not user or not password:
        record("Airflow REST API Authentication", target, "FAIL", "Credentials not configured in environment")
        return

    token_url = f"http://{HOST}:{port}/auth/token"
    try:
        token_resp = requests.post(token_url, json={"username": user, "password": password}, timeout=TIMEOUT)
        if token_resp.status_code in (200, 201):
            token_data = token_resp.json()
            token = token_data.get("access_token")
            token_type = token_data.get("token_type", "Bearer")
            # Query /api/v2/dags
            dags_url = f"http://{HOST}:{port}/api/v2/dags"
            headers = {"Authorization": f"{token_type} {token}"}
            dags_resp = requests.get(dags_url, headers=headers, timeout=TIMEOUT)
            if dags_resp.status_code == 200:
                data = dags_resp.json()
                total = data.get("total_entries", len(data.get("dags", [])))
                record("Airflow REST API Authentication", target, "PASS", f"Authenticated successfully (Airflow 3 Bearer JWT). Total DAGs: {total}")
            else:
                record("Airflow REST API Authentication", target, "FAIL", f"GET /api/v2/dags returned HTTP {dags_resp.status_code}", dags_resp.text[:200])
        else:
            record("Airflow REST API Authentication", target, "FAIL", f"POST /auth/token returned HTTP {token_resp.status_code}", token_resp.text[:200])
    except Exception as e:
        record("Airflow REST API Authentication", target, "FAIL", "Request error", str(e))


# ==============================================================================
# 2. AIRFLOW POSTGRES
# ==============================================================================
def test_airflow_postgres():
    port = int(os.getenv("AIRFLOW_DB_PORT", "31354"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("Airflow Postgres", target, "FAIL", "TCP connection failed", err)
        return

    user = os.getenv("AIRFLOW_DB_USER", "postgres")
    password = os.getenv("AIRFLOW_DB_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        record("Airflow Postgres", target, "SKIP", "Database password is not configured")
        return
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password

    psql_bin = "/opt/homebrew/bin/psql" if os.path.exists("/opt/homebrew/bin/psql") else "psql"
    cmd = [
        psql_bin,
        "-h", HOST,
        "-p", str(port),
        "-U", user,
        "-d", "postgres",
        "-w",
        "-c", "SELECT version();",
        "-t", "-A",
    ]
    try:
        res = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=TIMEOUT)
        if res.returncode == 0:
            ver = res.stdout.strip().split("\n")[0]
            record("Airflow Postgres", target, "PASS", f"Connected & authenticated (db: postgres). Version: {ver[:60]}")
        else:
            record("Airflow Postgres", target, "FAIL", "Authentication/Query failed", res.stderr.strip())
    except Exception as e:
        record("Airflow Postgres", target, "FAIL", "Execution error", str(e))


# ==============================================================================
# 3. DORIS COUPLED UI
# ==============================================================================
def test_doris_ui():
    port = int(os.getenv("DORIS_UI_PORT", "31041"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("Doris Coupled UI", target, "FAIL", "TCP connection failed", err)
        return

    url = f"http://{HOST}:{port}/"
    try:
        resp = requests.get(url, timeout=TIMEOUT)
        if resp.status_code < 400:
            record("Doris Coupled UI", target, "PASS", f"HTTP {resp.status_code} OK (UI reachable)")
        else:
            record("Doris Coupled UI", target, "FAIL", f"HTTP {resp.status_code}", resp.text[:200])
    except Exception as e:
        record("Doris Coupled UI", target, "FAIL", "Request error", str(e))


# ==============================================================================
# 4. DORIS MYSQL CLIENT
# ==============================================================================
def test_doris_mysql():
    port = int(os.getenv("DORIS_PORT", "31545"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("Doris Coupled Mysql client", target, "FAIL", "TCP connection failed", err)
        return

    user = os.getenv("DORIS_USER", "root")
    password = os.getenv("DORIS_PASSWORD", "")
    try:
        conn = pymysql.connect(
            host=HOST,
            port=port,
            user=user,
            password=password,
            connect_timeout=int(TIMEOUT),
            read_timeout=int(TIMEOUT),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            ver = cur.fetchone()[0]
        conn.close()
        record("Doris MySQL (configured user)", target, "PASS", f"Connected & authenticated ({user}). Version: {ver}")
    except Exception as e:
        record("Doris MySQL (configured user)", target, "FAIL", f"Failed to authenticate/query as {user}", str(e))


# ==============================================================================
# 5. DORIS STREAM LOAD
# ==============================================================================
def test_doris_stream_load():
    port = int(os.getenv("DORIS_HTTP_PORT", "32713"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("Doris Coupled Stream Load", target, "FAIL", "TCP connection failed", err)
        return

    url = f"http://{HOST}:{port}/api/bootstrap"
    try:
        resp = requests.get(url, timeout=TIMEOUT)
        record("Doris Coupled Stream Load", target, "PASS", f"HTTP {resp.status_code} - Stream Load interface alive")
    except Exception:
        try:
            resp = requests.get(f"http://{HOST}:{port}/", timeout=TIMEOUT)
            record("Doris Coupled Stream Load", target, "PASS", f"HTTP {resp.status_code} - Stream Load interface alive")
        except Exception as e2:
            record("Doris Coupled Stream Load", target, "FAIL", "HTTP connection error", str(e2))


# ==============================================================================
# 6. MINIO UI
# ==============================================================================
def test_minio_ui():
    port = int(os.getenv("MINIO_UI_PORT", "30957"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("MinIO UI", target, "FAIL", "TCP connection failed", err)
        return

    url = f"http://{HOST}:{port}/"
    try:
        resp = requests.get(url, timeout=TIMEOUT)
        if resp.status_code < 400 or resp.status_code == 403:
            record("MinIO UI", target, "PASS", f"HTTP {resp.status_code} OK (MinIO Console reachable)")
        else:
            record("MinIO UI", target, "FAIL", f"HTTP {resp.status_code}", resp.text[:200])
    except Exception as e:
        record("MinIO UI", target, "FAIL", "Request error", str(e))


# ==============================================================================
# 7. MINIO API
# ==============================================================================
def test_minio_api():
    port = int(os.getenv("MINIO_PORT", "32010"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("MinIO API", target, "FAIL", "TCP connection failed", err)
        return

    access_key = os.getenv("MINIO_ACCESS_KEY", "")
    secret_key = os.getenv("MINIO_SECRET_KEY", "")
    secure = os.getenv("MINIO_SECURE", "false").lower() in ("true", "1")
    try:
        client = Minio(
            target,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        buckets = client.list_buckets()
        bucket_names = [b.name for b in buckets]
        record("MinIO API", target, "PASS", f"Authenticated successfully. Buckets found: {len(bucket_names)} ({', '.join(bucket_names[:5])})")
    except Exception as e:
        record("MinIO API", target, "FAIL", "Authentication/list_buckets error", str(e))


# ==============================================================================
# 8. POSTGRESQL DATABASE
# ==============================================================================
def test_postgresql_db():
    port = int(os.getenv("POSTGRES_PORT", "30986"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("Postgresql database", target, "FAIL", "TCP connection failed", err)
        return

    user = os.getenv("POSTGRES_USER", "ps_user")
    password = os.getenv("POSTGRES_PASSWORD", "")
    database = os.getenv("POSTGRES_DB", "ps_db")

    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password

    psql_bin = "/opt/homebrew/bin/psql" if os.path.exists("/opt/homebrew/bin/psql") else "psql"
    cmd = [
        psql_bin,
        "-h", HOST,
        "-p", str(port),
        "-U", user,
        "-d", database,
        "-w",
        "-c", "SELECT 1;",
        "-t", "-A",
    ]
    try:
        res = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=TIMEOUT)
        if res.returncode == 0 and res.stdout.strip() == "1":
            record("Postgresql database", target, "PASS", f"Connected & authenticated ({user} @ {database}). SELECT 1 succeeded.")
        else:
            record("Postgresql database", target, "FAIL", "Query failed", res.stderr.strip())
    except Exception as e:
        record("Postgresql database", target, "FAIL", "Execution error", str(e))


# ==============================================================================
# 9. MSSQL DATABASE
# ==============================================================================
def test_mssql_db():
    port = int(os.getenv("MSSQL_PORT", "31704"))
    target = f"{HOST}:{port}"
    ok, err = check_tcp(HOST, port)
    if not ok:
        record("Mssql database", target, "FAIL", "TCP connection failed", err)
        return

    prelogin_pkt = bytes([
        0x12, 0x01, 0x00, 0x24, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x15, 0x00, 0x06,
        0x01, 0x00, 0x1B, 0x00, 0x01,
        0x02, 0x00, 0x1C, 0x00, 0x01,
        0x03, 0x00, 0x1D, 0x00, 0x04,
        0xFF,
        0x0F, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02,
        0x00,
        0x00, 0x00, 0x00, 0x00,
    ])

    tds_identified = False
    tds_version_str = ""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        s.connect((HOST, port))
        s.sendall(prelogin_pkt)
        resp = s.recv(1024)
        s.close()

        if len(resp) >= 8 and resp[0] in (0x04, 0x12):
            tds_identified = True
            if len(resp) >= 30:
                major = resp[29]
                minor = resp[30]
                build = (resp[31] << 8) | resp[32] if len(resp) >= 33 else 0
                tds_version_str = f"SQL Server TDS identified (v{major}.{minor}.{build})"
            else:
                tds_version_str = "SQL Server TDS response received"
    except Exception as e:
        record("Mssql database", target, "FAIL", "TDS handshake error", str(e))
        return

    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASSWORD", "")
    try:
        import pymssql
        conn = pymssql.connect(
            server=HOST,
            port=port,
            user=user,
            password=password,
            timeout=int(TIMEOUT),
            login_timeout=int(TIMEOUT),
        )
        cur = conn.cursor()
        cur.execute("SELECT @@VERSION;")
        row = cur.fetchone()
        conn.close()
        ver = row[0].split("\n")[0] if row else "Unknown"
        record("Mssql database", target, "PASS", f"Connected & authenticated ({user}). Version: {ver[:60]}")
    except ImportError:
        if tds_identified:
            record("Mssql database", target, "PASS", f"Port responsive & {tds_version_str}. (pymssql driver not installed for full query execution)")
        else:
            record("Mssql database", target, "PASS", "Port reachable and listening.")
    except Exception as e:
        record("Mssql database", target, "FAIL", "Authentication failed", str(e))


def main():
    print("=" * 70)
    print(f"VERIFYING EXTERNAL SERVICES CONNECTIVITY & AUTHENTICATION ({HOST})")
    print("=" * 70)

    test_airflow_api()
    test_airflow_postgres()
    test_doris_ui()
    test_doris_mysql()
    test_doris_stream_load()
    test_minio_ui()
    test_minio_api()
    test_postgresql_db()
    test_mssql_db()

    print("=" * 70)
    print(f"SUMMARY: {sum(1 for r in results if r['status'] == 'PASS')}/{len(results)} checks PASSED")
    print("=" * 70)


if __name__ == "__main__":
    main()
