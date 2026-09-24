import gzip
import io
import logging
from typing import Dict, List, Tuple
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

try:
    from task2_table_loader.src.config import LoaderConfig
except ImportError:
    from src.config import LoaderConfig

logger = logging.getLogger(__name__)


class RawTableLoader:
    """Handles decompression, parsing, row validation, deduplication, and Doris database writes."""

    def __init__(self, config: LoaderConfig) -> None:
        self.config = config
        self.doris_hook = MySqlHook(mysql_conn_id=config.doris_conn_id)

    def create_tables_if_not_exist(self) -> None:
        """Initializes all necessary Doris tables once before file ingestion begins."""
        # 1. Audit Table
        create_audit_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.audit_table}` (
            `file_name` VARCHAR(255) NOT NULL,
            `records_loaded` INT DEFAULT 0,
            `records_rejected` INT DEFAULT 0,
            `loaded_at` DATETIME NOT NULL
        )
        UNIQUE KEY(`file_name`)
        DISTRIBUTED BY HASH(`file_name`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        self.doris_hook.run(create_audit_sql)

        # 2. Reject Table
        create_reject_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.reject_table}` (
            `iccid` VARCHAR(64) NOT NULL,
            `file_name` VARCHAR(255) NOT NULL,
            `reject_reason` VARCHAR(100) NOT NULL,
            `companyid` VARCHAR(64),
            `customernumber` VARCHAR(64),
            `raw_row_record` TEXT,
            `rejected_at` DATETIME NOT NULL
        )
        DUPLICATE KEY(`iccid`, `file_name`)
        DISTRIBUTED BY HASH(`iccid`) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        self.doris_hook.run(create_reject_sql)

        # 3. Target Raw Table
        # Drop stale table if schema is mismatched from previous test runs
        self.doris_hook.run(f"DROP TABLE IF EXISTS `{self.config.target_table}` FORCE;")

        cols_ddl = [f"`{col}` VARCHAR(255)" for col in self.config.expected_columns if col != "iccid"]
        create_target_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.target_table}` (
            `iccid` VARCHAR(64) NOT NULL,
            {', '.join(cols_ddl)},
            `file_name` VARCHAR(255),
            `loaded_at` DATETIME
        )
        DUPLICATE KEY(`iccid`)
        DISTRIBUTED BY HASH(`iccid`) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        self.doris_hook.run(create_target_sql)
        logger.info("Doris tables verified and created successfully.")

    def is_file_processed(self, file_name: str) -> bool:
        """Checks if the file was already loaded into Doris."""
        try:
            sql = f"SELECT 1 FROM `{self.config.audit_table}` WHERE `file_name` = %s LIMIT 1;"
            records = self.doris_hook.get_records(sql, parameters=(file_name,))
            return bool(records)
        except Exception:
            return False

    def parse_content(self, file_name: str, raw_bytes: bytes) -> pd.DataFrame:
        """Decompresses gzip files if necessary and parses semicolon-delimited CSV into a DataFrame."""
        if file_name.endswith(".gz"):
            raw_bytes = gzip.decompress(raw_bytes)

        df = pd.read_csv(
            io.BytesIO(raw_bytes),
            sep=";",
            dtype=str,
            keep_default_na=False,
            on_bad_lines="skip"
        )

        df.columns = [
            col.strip().lower().replace(" ", "").replace("_", "")
            for col in df.columns
        ]

        expected = self.config.expected_columns
        normalized_map = {col.replace("_", ""): col for col in expected}
        df = df.rename(columns=normalized_map)
        df = df.reindex(columns=expected, fill_value="")

        return df

    def validate_and_filter(
        self, df: pd.DataFrame, file_name: str, load_timestamp: str
    ) -> Tuple[pd.DataFrame, List[Tuple]]:
        """Applies row validation rules, routes bad rows to rejects, and deduplicates valid rows."""
        valid_rows: List[Dict] = []
        rejected_rows: List[Tuple] = []

        for _, row in df.iterrows():
            iccid = str(row.get("iccid", "")).strip()
            customer_no = str(row.get("customernumber", "")).strip()
            company_id = str(row.get("companyid", "")).strip()

            reject_reason = None

            if not iccid or iccid.lower() in ("nan", "none"):
                reject_reason = "NULL_OR_EMPTY_ICCID"
            elif not customer_no or customer_no.lower() in ("nan", "none"):
                reject_reason = "NULL_OR_EMPTY_CUSTOMER_NUMBER"
            elif not company_id or company_id in ("0", "0.0") or company_id.lower() in ("nan", "none"):
                reject_reason = "NULL_OR_ZERO_COMPANY_ID"

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
                valid_rows.append(row.to_dict())

        if valid_rows:
            valid_df = pd.DataFrame(valid_rows)
            valid_df = valid_df.drop_duplicates(subset=["iccid", "customernumber"], keep="last")
            valid_df["file_name"] = file_name
            valid_df["loaded_at"] = load_timestamp
        else:
            valid_df = pd.DataFrame()

        return valid_df, rejected_rows

    def load_records(
        self, valid_df: pd.DataFrame, rejected_rows: List[Tuple], file_name: str, load_timestamp: str
    ) -> None:
        """Executes transactional bulk inserts into target, rejection, and audit tables."""
        conn = self.doris_hook.get_conn()
        cursor = conn.cursor()

        try:
            # 1. Insert valid rows into the target table
            if not valid_df.empty:
                ordered_cols = ["iccid"] + [c for c in valid_df.columns if c != "iccid"]
                valid_df = valid_df[ordered_cols]

                escaped_cols = [f"`{c}`" for c in ordered_cols]
                placeholders = ", ".join(["%s"] * len(ordered_cols))

                insert_sql = (
                    f"INSERT INTO `{self.config.target_table}` ({', '.join(escaped_cols)}) "
                    f"VALUES ({placeholders});"
                )
                cursor.executemany(insert_sql, [tuple(r) for r in valid_df.to_numpy()])

            # 2. Insert rejected rows into the rejection table
            if rejected_rows:
                reject_sql = f"""
                    INSERT INTO `{self.config.reject_table}`
                    (`iccid`, `file_name`, `reject_reason`, `companyid`, `customernumber`, `raw_row_record`, `rejected_at`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                cursor.executemany(reject_sql, rejected_rows)

            # 3. Insert processing summary into the audit tracking table
            audit_sql = f"""
                INSERT INTO `{self.config.audit_table}`
                (`file_name`, `records_loaded`, `records_rejected`, `loaded_at`)
                VALUES (%s, %s, %s, %s);
            """
            cursor.execute(
                audit_sql,
                (file_name, len(valid_df), len(rejected_rows), load_timestamp)
            )

            conn.commit()
            logger.info(
                "Ingestion completed for %s: %d valid loaded, %d rejected",
                file_name, len(valid_df), len(rejected_rows)
            )

        except Exception as exc:
            conn.rollback()
            logger.error("Failed to load records for %s: %s", file_name, str(exc))
            raise exc

        finally:
            cursor.close()
            conn.close()