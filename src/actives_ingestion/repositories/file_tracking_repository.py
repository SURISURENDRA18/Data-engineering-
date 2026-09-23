"""File Tracking Repository for actives_file_tracking table in Doris."""

import time
from datetime import datetime, timezone
from typing import Any

import pymysql

from actives_ingestion.config import DorisConfig, pipeline_config
from actives_ingestion.constants import STATUS_SUCCESS
from actives_ingestion.utils import get_logger

logger = get_logger("file_tracking_repository")


class FileTrackingRepository:
    """Manages file processing state in Doris actives_file_tracking table.

    Provides idempotency guarantees, prevents duplicate file loads, and
    maintains per-region sequence tracking.
    """

    def __init__(self, config: DorisConfig | None = None, connection=None, in_memory: bool = False):
        self.config = config or pipeline_config.doris
        self._connection = connection
        self.in_memory = in_memory

        # In-memory storage for offline / unit test scenarios
        self._mem_records: dict[str, dict[str, Any]] = {}
        self._mem_region_sequences: dict[str, set[int]] = {}

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

    def is_file_processed(self, file_hash: str) -> bool:
        """Check if file hash has already been successfully processed (idempotency check)."""
        if self.in_memory:
            record = self._mem_records.get(file_hash)
            return record is not None and record.get("status") == STATUS_SUCCESS

        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                sql = f"""
                    SELECT status FROM {self.config.database}.{self.config.tracking_table}
                    WHERE file_hash = %s
                """
                cursor.execute(sql, (file_hash,))
                result = cursor.fetchone()
                return result is not None and result.get("status") == STATUS_SUCCESS
        except Exception as e:
            logger.error("Failed to check processed status from Doris: %s", e)
            raise

    def validate_sequence(self, region: str, sequence_number: int) -> bool:
        """Validate sequence number for region against previously successfully processed files.

        Rule:
        - Sequence must not be previously processed for the same region (no repeats).
        - Sequence must be strictly greater than the latest processed sequence (strictly increasing).
        - If no previous sequence exists for the region, any positive sequence (>= 1) is accepted.
        """
        if sequence_number < 1:
            return False

        if self.in_memory:
            seen_seqs = self._mem_region_sequences.get(region, set())
            if sequence_number in seen_seqs:
                return False
            if seen_seqs and sequence_number <= max(seen_seqs):
                return False
            return True

        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                sql = f"""
                    SELECT sequence_number FROM {self.config.database}.{self.config.tracking_table}
                    WHERE region = %s AND status = '{STATUS_SUCCESS}'
                    ORDER BY sequence_number DESC
                """
                cursor.execute(sql, (region,))
                rows = cursor.fetchall()
                if not rows:
                    return True
                processed_seqs = [r["sequence_number"] for r in rows if r["sequence_number"] is not None]
                if not processed_seqs:
                    return True
                if sequence_number in processed_seqs:
                    return False
                if sequence_number <= max(processed_seqs):
                    return False
                return True
        except Exception as e:
            logger.error("Failed to query sequence history from Doris: %s", e)
            raise

    def record_status(
        self,
        file_hash: str,
        file_name: str,
        status: str,
        region: str | None = None,
        sequence_number: int | None = None,
        rows_loaded: int = 0,
        rows_rejected: int = 0,
        error_message: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        self._mem_records[file_hash] = {
            "file_hash": file_hash,
            "file_name": file_name,
            "region": region,
            "sequence_number": sequence_number,
            "status": status,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
            "processed_at": now,
            "error_message": error_message,
        }
        if status == STATUS_SUCCESS and region and sequence_number is not None:
            self._mem_region_sequences.setdefault(region, set()).add(sequence_number)

        if not self.in_memory:
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    conn = self._get_connection()
                    with conn.cursor() as cursor:
                        sql = f"""
                            INSERT INTO {self.config.database}.{self.config.tracking_table}
                            (file_hash, file_name, region, sequence_number, status, rows_loaded, rows_rejected, processed_at, error_message)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor.execute(
                            sql,
                            (
                                file_hash,
                                file_name,
                                region,
                                sequence_number,
                                status,
                                rows_loaded,
                                rows_rejected,
                                now.strftime("%Y-%m-%d %H:%M:%S"),
                                error_message or "",
                            ),
                        )
                    break
                except Exception as e:
                    if attempt == max_retries:
                        logger.error("Failed to write tracking record to Doris after %s attempts: %s", max_retries, e)
                        raise
                    else:
                        time.sleep(0.5 * attempt)
