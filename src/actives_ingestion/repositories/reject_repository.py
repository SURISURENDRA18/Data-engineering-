"""Repository for raw_actives_daily_rejected table."""

from typing import Any

import pymysql

from actives_ingestion.config import DorisConfig, pipeline_config
from actives_ingestion.models import RejectedRow
from actives_ingestion.utils import chunk_list, get_logger

logger = get_logger("reject_repository")


class RejectRepository:
    """Handles persistence of rejected rows into Doris raw_actives_daily_rejected."""

    def __init__(self, config: DorisConfig | None = None, connection=None, in_memory: bool = False):
        self.config = config or pipeline_config.doris
        self._connection = connection
        self.in_memory = in_memory
        self._mem_rejected: list[dict[str, Any]] = []

    def _get_connection(self):
        """Get or create a PyMySQL connection to Doris using configured credentials."""
        if self._connection:
            return self._connection
        try:
            return pymysql.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                connect_timeout=self.config.connect_timeout,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
            )
        except pymysql.err.OperationalError:
            raise

    def insert_batch(self, rejected_rows: list[RejectedRow]) -> int:
        """Insert a batch of rejected rows into Doris using chunked bulk INSERT.

        Idempotency guarantee: Cleans up any previously recorded rejected rows for
        the given file hash before inserting new records.
        """
        if not rejected_rows:
            return 0

        target_hashes = {r.file_hash for r in rejected_rows}
        self._mem_rejected = [m for m in self._mem_rejected if m["file_hash"] not in target_hashes]

        for r in rejected_rows:
            self._mem_rejected.append({
                "id": r.id,
                "file_name": r.file_name,
                "file_hash": r.file_hash,
                "row_number": r.row_number,
                "reason": r.reason,
                "iccid": r.iccid,
                "customernumber": r.customernumber,
                "companyid": r.companyid,
                "raw_row": r.raw_row,
                "rejected_at": r.rejected_at,
            })

        if self.in_memory:
            return len(rejected_rows)

        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                # Delete any existing rejected rows for these files to ensure exact rerun idempotency
                for f_hash in target_hashes:
                    del_sql = f"DELETE FROM {self.config.database}.{self.config.rejected_table} WHERE file_hash = %s"
                    cursor.execute(del_sql, (f_hash,))

                sql = f"""
                    INSERT INTO {self.config.database}.{self.config.rejected_table}
                    (id, file_name, file_hash, row_number, reason, iccid, customernumber, companyid, raw_row, rejected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                total_inserted = 0
                for chunk in chunk_list(rejected_rows, 1000):
                    data = [
                        (
                            r.id,
                            r.file_name,
                            r.file_hash,
                            r.row_number,
                            r.reason,
                            r.iccid,
                            r.customernumber,
                            r.companyid,
                            r.raw_row,
                            r.rejected_at.strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        for r in chunk
                    ]
                    cursor.executemany(sql, data)
                    total_inserted += len(chunk)
            return total_inserted
        except Exception as e:
            logger.error("Failed to batch insert rejected rows into Doris: %s", e)
            raise

    def get_rejected_rows(self) -> list[dict[str, Any]]:
        """Return rejected rows recorded in memory (useful for testing and auditing)."""
        return list(self._mem_rejected)
