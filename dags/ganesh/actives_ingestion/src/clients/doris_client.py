"""Thin PyMySQL wrapper for Doris (MySQL wire protocol). This is the only
module that touches the Doris Airflow Connection directly; everything
above it works with a DB-API-ish connection object, so tests can inject
a fake in-memory one instead.
"""

import functools
import os
from pathlib import Path
from typing import Any, List, Optional, Sequence

import pymysql
import pymysql.cursors

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def get_doris_connection(conn_id: str, database: Optional[str] = None):
    """Builds a PyMySQL connection from an Airflow Connection (preferred) or,
    if Airflow / the connection isn't available (local/unit-test runs),
    from DORIS_* environment variables."""
    host = port = user = password = None

    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        host, port, user, password = conn.host, conn.port, conn.login, conn.password
    except Exception:
        host = os.environ.get("DORIS_HOST")
        port = int(os.environ.get("DORIS_PORT", "9030"))
        user = os.environ.get("DORIS_USER", "root")
        password = os.environ.get("DORIS_PASSWORD", "")

    if not host:
        raise RuntimeError(
            f"No Doris connection available for conn_id={conn_id!r} and no "
            "DORIS_HOST fallback set."
        )

    return pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password or "",
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


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
