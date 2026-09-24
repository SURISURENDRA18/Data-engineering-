"""Thin PyMySQL wrapper for Doris (MySQL wire protocol). This is the only
module that touches the Doris Airflow Connection directly; everything
above it works with a DB-API-ish connection object, so tests can inject
a fake in-memory one instead.
"""

import functools
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pymysql
import pymysql.cursors
import requests

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def get_doris_connection_params(conn_id: str) -> Dict[str, Any]:
    """Resolves {host, port, user, password} from an Airflow Connection
    (preferred) or, if Airflow / the connection isn't available
    (local/unit-test runs), from DORIS_* environment variables. Shared by
    the MySQL-protocol connection and the separate Stream Load HTTP
    endpoint (same host/credentials, different port)."""
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        return {"host": conn.host, "port": conn.port, "user": conn.login, "password": conn.password or ""}
    except Exception:
        return {
            "host": os.environ.get("DORIS_HOST"),
            "port": int(os.environ.get("DORIS_PORT", "9030")),
            "user": os.environ.get("DORIS_USER", "root"),
            "password": os.environ.get("DORIS_PASSWORD", ""),
        }


def get_doris_connection(conn_id: str, database: Optional[str] = None):
    """Builds a PyMySQL connection from an Airflow Connection (preferred) or,
    if Airflow / the connection isn't available (local/unit-test runs),
    from DORIS_* environment variables."""
    params = get_doris_connection_params(conn_id)
    if not params["host"]:
        raise RuntimeError(
            f"No Doris connection available for conn_id={conn_id!r} and no "
            "DORIS_HOST fallback set."
        )

    return pymysql.connect(
        host=params["host"],
        port=int(params["port"]),
        user=params["user"],
        password=params["password"] or "",
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def stream_load(
    host: str,
    http_port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    csv_body: bytes,
    columns: List[str],
    label: str,
    column_separator: str = "\t",
    timeout: int = 300,
) -> dict:
    """Bulk-loads csv_body into a Doris table via Stream Load -- Doris's
    recommended high-throughput ingestion path, far faster than batched
    INSERT for large row counts. `label` should be deterministic per
    distinct file content (e.g. its checksum): retrying the same label
    with the same data is a safe no-op ("Label Already Exists"), giving
    an extra layer of idempotency on top of file_processing_log.

    Handles the FE->BE redirect manually rather than relying on
    `requests`'s automatic redirect following: `requests` strips the
    Authorization header on cross-host redirects (a security default),
    which would silently break Basic Auth on Stream Load's FE->BE hop.
    """
    url = f"http://{host}:{http_port}/api/{database}/{table}/_stream_load"
    headers = {
        "Expect": "100-continue",
        "label": label,
        "column_separator": column_separator,
        "columns": ",".join(columns),
        "format": "csv",
        "Content-Type": "text/plain",
    }
    auth = (user, password)

    response = requests.put(url, data=csv_body, headers=headers, auth=auth, allow_redirects=False, timeout=timeout)
    if response.status_code in (301, 302, 307, 308) and "Location" in response.headers:
        response = requests.put(
            response.headers["Location"], data=csv_body, headers=headers, auth=auth, allow_redirects=False, timeout=timeout
        )

    try:
        result = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Stream Load returned a non-JSON response (status {response.status_code}): {response.text[:500]}") from exc

    status = result.get("Status")
    if status not in ("Success", "Publish Timeout", "Label Already Exists"):
        raise RuntimeError(f"Stream Load failed for label={label!r}: {result}")
    if result.get("NumberFilteredRows", 0):
        raise RuntimeError(f"Stream Load filtered {result['NumberFilteredRows']} row(s) for label={label!r}: {result}")
    return result


@functools.lru_cache(maxsize=None)
def load_sql_file(relative_path: str) -> str:
    return (SQL_DIR / relative_path).read_text()


def render_sql(relative_path: str, **template_vars) -> str:
    """Loads a .sql file and substitutes ``${NAME}`` placeholders (used only
    for identifiers like database names, never for row values — row values
    always go through parameterized queries)."""
    text = load_sql_file(relative_path)
    for key, value in template_vars.items():
        text = text.replace(f"${{{key}}}", value)
    return text


def run_ddl_script(conn, sql_text: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(sql_text)


def execute_query(conn, sql: str, params: Optional[Sequence[Any]] = None) -> List[dict]:
    with conn.cursor() as cursor:
        cursor.execute(sql, params or ())
        return list(cursor.fetchall())


def execute_write(conn, sql: str, params: Optional[Sequence[Any]] = None) -> int:
    with conn.cursor() as cursor:
        return cursor.execute(sql, params or ())


def executemany_write(conn, sql: str, param_list: Sequence[Sequence[Any]], batch_size: int = 1000) -> int:
    if not param_list:
        return 0
    total = 0
    with conn.cursor() as cursor:
        for start in range(0, len(param_list), batch_size):
            batch = param_list[start : start + batch_size]
            total += cursor.executemany(sql, batch)
    return total
