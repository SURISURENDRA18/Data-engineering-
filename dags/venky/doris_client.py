from __future__ import annotations

from airflow.hooks.base import BaseHook
import pymysql

from .config import DORIS_CONN_ID


def get_connection():
    conn = BaseHook.get_connection(DORIS_CONN_ID)

    return pymysql.connect(
        host=conn.host,
        port=conn.port or 9030,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        charset="utf8mb4",
        autocommit=True,
    )


def execute(query: str, params=None):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    finally:
        connection.close()


def execute_many(query: str, rows):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, rows)

        connection.commit()

    finally:
        connection.close()