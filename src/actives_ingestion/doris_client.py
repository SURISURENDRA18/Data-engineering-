"""Doris client for executing schema DDL, schema verification, and bulk data ingestion via Stream Load."""

import json
import os
import re
from typing import Any

import pymysql
import requests
from requests.auth import HTTPBasicAuth

from actives_ingestion.config import DorisConfig, get_airflow_connection_details, pipeline_config
from actives_ingestion.utils import chunk_list, get_logger

logger = get_logger("doris_client")


class DorisClient:
    """Provides DDL management, schema verification, and high-throughput ingestion via Doris Stream Load."""

    def __init__(self, config: DorisConfig | None = None, connection=None, in_memory: bool = False):
        self.config = config or pipeline_config.doris
        self._connection = connection
        self.in_memory = in_memory

        # In-memory storage for testing and offline execution
        self._mem_raw_table: list[dict[str, Any]] = []

    def _get_connection(self):
        """Get or create a PyMySQL connection using configured credentials."""
        if self._connection:
            return self._connection
        try:
            connection = get_airflow_connection_details("doris_default")
            host = self.config.host
            port = self.config.port
            user = self.config.user
            password = self.config.password
            if connection:
                host = str(connection["host"])
                port = int(connection["port"] or port)
                user = str(connection["login"])
                password = str(connection["password"])
            return pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                connect_timeout=self.config.connect_timeout,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
            )
        except pymysql.err.OperationalError:
            raise

    def get_table_unique_keys(self, table_name: str) -> list[str]:
        """Extract UNIQUE KEY column names from Doris SHOW CREATE TABLE."""
        if self.in_memory:
            return ["customernumber", "iccid"]

        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW CREATE TABLE {self.config.database}.{table_name}")
            row = cursor.fetchone()
            if not row:
                return []
            create_sql = row.get("Create Table", "")
            # Match UNIQUE KEY(`col1`, `col2`, ...) or UNIQUE KEY(col1, col2, ...)
            match = re.search(r"UNIQUE KEY\s*\(([^)]+)\)", create_sql, re.IGNORECASE)
            if match:
                cols_str = match.group(1)
                keys = [c.strip().strip("`").lower() for c in cols_str.split(",")]
                return keys
            return []

    def verify_schema(self) -> dict[str, Any]:
        """Verify existence of database, schema tables, and required key contracts."""
        if self.in_memory:
            return {
                "database_exists": True,
                "tables": {
                    self.config.raw_table: {"exists": True, "unique_keys": ["customernumber", "iccid"], "is_valid": True},
                    self.config.rejected_table: {"exists": True, "is_valid": True},
                    self.config.tracking_table: {"exists": True, "unique_keys": ["file_hash"], "is_valid": True},
                },
                "all_valid": True,
            }

        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES;")
            dbs = [d["Database"].lower() for d in cursor.fetchall()]
            if self.config.database.lower() not in dbs:
                return {"database_exists": False, "tables": {}, "all_valid": False}

            cursor.execute(f"SHOW TABLES FROM {self.config.database};")
            tables = [list(t.values())[0].lower() for t in cursor.fetchall()]

            raw_exists = self.config.raw_table.lower() in tables
            raw_keys = self.get_table_unique_keys(self.config.raw_table) if raw_exists else []
            # Required composite key: customernumber and iccid in either order (standard is customernumber, iccid)
            raw_valid = raw_exists and set(raw_keys) == {"customernumber", "iccid"}

            rej_exists = self.config.rejected_table.lower() in tables
            tracking_exists = self.config.tracking_table.lower() in tables
            tracking_keys = self.get_table_unique_keys(self.config.tracking_table) if tracking_exists else []
            tracking_valid = tracking_exists and "file_hash" in tracking_keys

            all_valid = raw_valid and rej_exists and tracking_valid

            return {
                "database_exists": True,
                "tables": {
                    self.config.raw_table: {"exists": raw_exists, "unique_keys": raw_keys, "is_valid": raw_valid},
                    self.config.rejected_table: {"exists": rej_exists, "is_valid": rej_exists},
                    self.config.tracking_table: {"exists": tracking_exists, "unique_keys": tracking_keys, "is_valid": tracking_valid},
                },
                "all_valid": all_valid,
            }

    def align_schema_if_needed(self, sql_dir: str | None = None) -> bool:
        """Verify the raw table key without mutating an existing table.

        Schema migrations require an explicit, reviewed operator action.
        """
        if self.in_memory:
            return True

        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES FROM {self.config.database} LIKE '{self.config.raw_table}';")
            if not cursor.fetchone():
                return False

            unique_keys = self.get_table_unique_keys(self.config.raw_table)
            if set(unique_keys) == {"customernumber", "iccid"}:
                logger.info("Table %s already has required UNIQUE KEY(customernumber, iccid).", self.config.raw_table)
                return True

            logger.error(
                "Table %s has incorrect UNIQUE KEY %s; refusing automatic schema mutation.",
                self.config.raw_table,
                unique_keys,
            )
            return False

    def init_database_and_tables(self, sql_dir: str | None = None) -> None:
        """Create database and schema tables if they do not exist, and verify physical schema."""
        if self.in_memory:
            return

        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")

                base_dir = sql_dir or os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "sql"
                )
                ddl_files = [
                    "create_raw_table.sql",
                    "create_rejected_table.sql",
                    "create_file_tracking_table.sql",
                ]

                for ddl_file in ddl_files:
                    path = os.path.join(base_dir, ddl_file)
                    if os.path.exists(path):
                        with open(path, "r", encoding="utf-8") as f:
                            sql = f.read().replace("{database_name}", self.config.database)
                            for stmt in sql.split(";"):
                                stmt_clean = stmt.strip()
                                if stmt_clean:
                                    cursor.execute(stmt_clean)

            # Verify the raw key without silently dropping, renaming, or replacing data.
            self.align_schema_if_needed(sql_dir)

            schema_status = self.verify_schema()
            if not schema_status.get("all_valid"):
                raise RuntimeError(f"Doris schema does not match the pipeline contract: {schema_status}")
            else:
                logger.info("Doris database and tables successfully verified.")
        except Exception as e:
            logger.error("Error initializing/verifying Doris tables via MySQL: %s", e)
            raise

    def stream_load_raw(self, rows: list[dict[str, Any]]) -> int:
        """Stream bulk load validated rows into Doris raw_actives_daily.

        Attempts high-speed HTTP Stream Load first.
        Falls back to batched MySQL INSERT if HTTP Stream Load endpoint is unreachable.
        """
        if not rows:
            return 0

        # Mirror Doris UNIQUE KEY(customernumber, iccid) upsert semantics.
        existing_by_pair = {
            (r.get("customernumber"), r.get("iccid")): i
            for i, r in enumerate(self._mem_raw_table)
        }
        for row in rows:
            pair = (row.get("customernumber"), row.get("iccid"))
            if all(pair) and pair in existing_by_pair:
                self._mem_raw_table[existing_by_pair[pair]] = row
            else:
                self._mem_raw_table.append(row)
                if all(pair):
                    existing_by_pair[pair] = len(self._mem_raw_table) - 1

        if self.in_memory:
            return len(rows)

        loaded = 0
        for batch in chunk_list(rows, max(1, self.config_batch_size)):
            try:
                loaded += self._stream_load_http(batch)
            except Exception as e:
                logger.warning("Doris Stream Load HTTP failed (%s). Attempting batched MySQL INSERT fallback.", e)
                loaded += self._bulk_insert_mysql(batch)
        return loaded

    @property
    def config_batch_size(self) -> int:
        """Return the configured bounded load batch size."""
        return max(1, self.config.batch_size)

    def _stream_load_http(self, rows: list[dict[str, Any]]) -> int:
        """Perform Doris HTTP Stream Load using JSON format."""
        connection = get_airflow_connection_details("doris_default")
        host = self.config.host
        http_port = self.config.http_port
        user = self.config.user
        password = self.config.password
        if connection:
            host = str(connection["host"])
            user = str(connection["login"])
            password = str(connection["password"])
            extra = connection.get("extra") or {}
            http_port = int(extra.get("http_port") or extra.get("httpPort") or http_port)

        url = f"http://{host}:{http_port}/api/{self.config.database}/{self.config.raw_table}/_stream_load"
        headers = {
            "format": "json",
            "strip_outer_array": "true",
            "Expect": "100-continue",
        }
        body = json.dumps(rows).encode("utf-8")
        auth = HTTPBasicAuth(user, password)

        response = requests.put(
            url,
            data=body,
            headers=headers,
            auth=auth,
            timeout=30,
        )

        if response.status_code != 200:
            raise RuntimeError(f"Doris Stream Load HTTP returned status {response.status_code}: {response.text}")

        resp_json = response.json()
        status = resp_json.get("Status")
        if status not in ("Success", "OK"):
            raise RuntimeError(f"Doris Stream Load failed: {resp_json.get('Message', resp_json)}")

        loaded_rows = resp_json.get("NumberLoadedRows", len(rows))
        logger.info(f"Doris Stream Load succeeded. Loaded {loaded_rows} rows.")
        return loaded_rows

    def _bulk_insert_mysql(self, rows: list[dict[str, Any]]) -> int:
        """Batch insert raw active records via PyMySQL connection."""
        if not rows:
            return 0

        conn = self._get_connection()
        columns = list(rows[0].keys())
        # Quote column names for safety (e.g. `type`)
        cols_clause = ", ".join(f"`{col}`" for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {self.config.database}.{self.config.raw_table} ({cols_clause}) VALUES ({placeholders})"

        total = 0
        with conn.cursor() as cursor:
            for chunk in chunk_list(rows, self.config_batch_size):
                data = [tuple(row.get(col) for col in columns) for row in chunk]
                cursor.executemany(sql, data)
                total += len(chunk)
        return total

    def get_loaded_rows(self) -> list[dict[str, Any]]:
        """Return raw rows loaded in memory."""
        return list(self._mem_raw_table)
