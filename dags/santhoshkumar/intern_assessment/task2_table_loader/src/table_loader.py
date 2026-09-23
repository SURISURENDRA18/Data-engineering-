import gzip
import io
import logging
from typing import Dict, List, Tuple
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from src.config import LoaderConfig

# Initialize module logger
logger = logging.getLogger(__name__)


class RawTableLoader:
    """Handles decompression, parsing, row validation, deduplication, and Doris database writes."""

    def __init__(self, config: LoaderConfig) -> None:
        # Save pipeline configuration reference
        self.config = config
        # Initialize MySQLHook configured for Apache Doris via Airflow connection
        self.doris_hook = MySqlHook(mysql_conn_id=config.doris_conn_id)

    def is_file_processed(self, file_name: str) -> bool:
        """Enforces idempotency by checking if the file already exists in the audit table."""
        # 1. Audit Table (matches audit_sql insert query)
        create_audit_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.audit_table} (
            file_name VARCHAR(255) NOT NULL,
            records_loaded INT DEFAULT 0,
            records_rejected INT DEFAULT 0,
            loaded_at DATETIME NOT NULL
        )
        UNIQUE KEY(file_name)
        DISTRIBUTED BY HASH(file_name) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        self.doris_hook.run(create_audit_sql)

        # 2. Reject Table (matches reject_sql insert query)
        create_reject_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.reject_table} (
            iccid VARCHAR(64) NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            reject_reason VARCHAR(100) NOT NULL,
            companyid VARCHAR(64),
            customernumber VARCHAR(64),
            raw_row_record TEXT,
            rejected_at DATETIME NOT NULL
        )
        DUPLICATE KEY(iccid, file_name)
        DISTRIBUTED BY HASH(iccid) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        self.doris_hook.run(create_reject_sql)

        # 3. Target Raw Table (dynamic schema based on expected_columns + metadata)
        cols_ddl = [f"`{col}` VARCHAR(255)" for col in self.config.expected_columns if col != "iccid"]
        create_target_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.target_table} (
            iccid VARCHAR(64) NOT NULL,
            {', '.join(cols_ddl)},
            file_name VARCHAR(255),
            loaded_at DATETIME
        )
        DUPLICATE KEY(iccid)
        DISTRIBUTED BY HASH(iccid) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        self.doris_hook.run(create_target_sql)

        # 4. Check audit log for matching filename entry
        sql = f"SELECT 1 FROM {self.config.audit_table} WHERE file_name = %s LIMIT 1;"
        records = self.doris_hook.get_records(sql, parameters=(file_name,))
        return bool(records)

    def parse_content(self, file_name: str, raw_bytes: bytes) -> pd.DataFrame:
        """Decompresses gzip files if necessary and parses semicolon-delimited CSV into a DataFrame."""
        # Check if the file has a gzip extension and decompress if true
        if file_name.endswith(".gz"):
            raw_bytes = gzip.decompress(raw_bytes)

        # Parse CSV byte stream using semicolon delimiter and string types for all columns
        df = pd.read_csv(
            io.BytesIO(raw_bytes),
            sep=";",
            dtype=str,
            keep_default_na=False
        )

        # Clean and normalize incoming header names (strip spaces, lowercase, remove underscores)
        df.columns = [
            col.strip().lower().replace(" ", "").replace("_", "")
            for col in df.columns
        ]

        # Build column mapping against expected canonical schema
        expected = self.config.expected_columns
        normalized_map = {col.replace("_", ""): col for col in expected}
        df = df.rename(columns=normalized_map)

        # Reindex to force exact canonical columns, populating missing columns with empty string
        df = df.reindex(columns=expected, fill_value="")

        return df

    def validate_and_filter(
        self, df: pd.DataFrame, file_name: str, load_timestamp: str
    ) -> Tuple[pd.DataFrame, List[Tuple]]:
        """Applies row validation rules, routes bad rows to rejects, and deduplicates valid rows."""
        valid_rows: List[Dict] = []
        rejected_rows: List[Tuple] = []

        # Iterate over every row to evaluate mandatory fields
        for _, row in df.iterrows():
            # Extract key fields and trim whitespaces
            iccid = str(row["iccid"]).strip()
            customer_no = str(row["customernumber"]).strip()
            company_id = str(row["companyid"]).strip()

            reject_reason = None

            # Rule 1: ICCID must not be empty or literal NaN/None
            if not iccid or iccid.lower() in ("nan", "none"):
                reject_reason = "NULL_OR_EMPTY_ICCID"

            # Rule 2: Customer number must not be empty or literal NaN/None
            elif not customer_no or customer_no.lower() in ("nan", "none"):
                reject_reason = "NULL_OR_EMPTY_CUSTOMER_NUMBER"

            # Rule 3: Company ID must not be empty, zero, or literal NaN/None
            elif not company_id or company_id in ("0", "0.0") or company_id.lower() in ("nan", "none"):
                reject_reason = "NULL_OR_ZERO_COMPANY_ID"

            # Route to rejected buffer if a rule failed
            if reject_reason:
                rejected_rows.append((
                    iccid,
                    file_name,
                    reject_reason,
                    company_id,
                    customer_no,
                    ";".join([str(val) for val in row.values]),
                    load_timestamp
                ))
            else:
                # Route valid record to staging dictionary
                valid_rows.append(row.to_dict())

        # Perform in-batch deduplication on (iccid, customernumber) keeping the latest record
        if valid_rows:
            valid_df = pd.DataFrame(valid_rows)
            valid_df = valid_df.drop_duplicates(subset=["iccid", "customernumber"], keep="last")
            # Attach operational metadata columns
            valid_df["file_name"] = file_name
            valid_df["loaded_at"] = load_timestamp
        else:
            valid_df = pd.DataFrame()

        return valid_df, rejected_rows

    def load_records(
        self, valid_df: pd.DataFrame, rejected_rows: List[Tuple], file_name: str, load_timestamp: str
    ) -> None:
        """Executes transactional bulk inserts into target, rejection, and audit tables."""
        # Acquire MySQL/Doris connection and cursor from the hook
        conn = self.doris_hook.get_conn()
        cursor = conn.cursor()

        try:
            # 1. Insert valid rows into the target table
            if not valid_df.empty:
                # Order columns so iccid appears first to match Doris DUPLICATE KEY definition
                ordered_cols = ["iccid"] + [c for c in valid_df.columns if c != "iccid"]
                valid_df = valid_df[ordered_cols]

                # Escape reserved keyword 'type' with backticks
                escaped_cols = [f"`{c}`" if c == "type" else c for c in ordered_cols]
                placeholders = ", ".join(["%s"] * len(ordered_cols))

                insert_sql = (
                    f"INSERT INTO {self.config.target_table} ({', '.join(escaped_cols)}) "
                    f"VALUES ({placeholders});"
                )
                cursor.executemany(insert_sql, [tuple(r) for r in valid_df.to_numpy()])

            # 2. Insert rejected rows into the rejection table
            if rejected_rows:
                reject_sql = f"""
                    INSERT INTO {self.config.reject_table}
                    (iccid, file_name, reject_reason, companyid, customernumber, raw_row_record, rejected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                cursor.executemany(reject_sql, rejected_rows)

            # 3. Insert processing summary into the audit tracking table
            audit_sql = f"""
                INSERT INTO {self.config.audit_table}
                (file_name, records_loaded, records_rejected, loaded_at)
                VALUES (%s, %s, %s, %s);
            """
            cursor.execute(
                audit_sql,
                (file_name, len(valid_df), len(rejected_rows), load_timestamp)
            )

            # Commit the transaction to finalize all table changes
            conn.commit()
            logger.info(
                "Ingestion completed for %s: %d valid loaded, %d rejected",
                file_name, len(valid_df), len(rejected_rows)
            )

        except Exception as exc:
            # Rollback transaction on failure to preserve consistent state
            conn.rollback()
            logger.error("Failed to load records for %s: %s", file_name, str(exc))
            raise exc

        finally:
            # Cleanly close cursor and database connection
            cursor.close()
            conn.close()