"""Task 2 per-file processing: parse a validated file into rows, apply the
row-level rejection rules, deduplicate on iccid, and insert into the raw
table. Tracking-table I/O (load-status idempotency, marking LOADED /
LOAD_FAILED) is the caller's (raw_load_pipeline's) responsibility, so this
module stays a pure "given file bytes, produce rows" transformer that
either succeeds or raises.
"""

import io
from datetime import datetime, timezone
from typing import List, Tuple

import pandas as pd

from src.clients.doris_client import executemany_write, render_sql, run_ddl_script
from src.config.actives_schema import COLUMN_DB_MAP, CSV_DELIMITER, RAW_TABLE_DATA_COLUMNS, RowRejectReason
from src.config.settings import Settings
from src.loaders.rejected_row_loader import insert_rejected_rows
from src.models.validation_result import RowLoadResult
from src.validators.file_content_validator import decompress_if_gzip


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_raw_tables_exist(conn, settings: Settings) -> None:
    for ddl_file in (
        "ddl/001_create_raw_actives_daily.sql",
        "ddl/002_create_raw_actives_daily_rejected.sql",
    ):
        run_ddl_script(conn, render_sql(ddl_file, DORIS_DATABASE=settings.doris_database))


def read_validated_file_as_dataframe(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    content_bytes = decompress_if_gzip(raw_bytes, filename)
    text = content_bytes.decode("utf-8")
    df = pd.read_csv(io.StringIO(text), sep=CSV_DELIMITER, dtype=str, keep_default_na=False)
    df = df.rename(columns=COLUMN_DB_MAP)
    return df[RAW_TABLE_DATA_COLUMNS]


def _is_blank(series: pd.Series) -> pd.Series:
    return series.isna() | (series.astype(str).str.strip() == "")


def apply_row_level_rules(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Rejects a row on the first rule it fails, checked in the exact
    order given by the assessment: iccid, then customernumber, then
    companyid. 'companyid empty or zero' is interpreted as the trimmed
    value being '' or '0' exactly (documented README assumption)."""
    df = df.reset_index(drop=True)
    reason = pd.Series([None] * len(df), index=df.index, dtype=object)

    iccid_bad = _is_blank(df["iccid"])
    reason[iccid_bad] = RowRejectReason.NULL_OR_EMPTY_ICCID

    customer_bad = _is_blank(df["customernumber"]) & reason.isna()
    reason[customer_bad] = RowRejectReason.NULL_OR_EMPTY_CUSTOMER_NUMBER

    company_values = df["companyid"].astype(str).str.strip()
    company_bad = company_values.isin(["", "0"]) & reason.isna()
    reason[company_bad] = RowRejectReason.NULL_OR_ZERO_COMPANY_ID

    rejected_mask = reason.notna()
    rejected_df = df[rejected_mask].copy()
    rejected_df["reject_reason"] = reason[rejected_mask]
    accepted_df = df[~rejected_mask].copy()
    return accepted_df, rejected_df


def dedup_rows(df: pd.DataFrame, key_column: str = "iccid") -> Tuple[pd.DataFrame, int]:
    """'Latest' = last physical row order in the file (no timestamp column
    exists to sort by) -- an explicit, documented, deterministic tie-break."""
    before = len(df)
    deduped = df.drop_duplicates(subset=[key_column], keep="last")
    return deduped, before - len(deduped)


def build_insert_rows(df: pd.DataFrame, file_name: str, loaded_at: datetime) -> List[tuple]:
    rows = []
    for _, row in df.iterrows():
        rows.append(tuple([file_name] + [row[col] for col in RAW_TABLE_DATA_COLUMNS] + [loaded_at]))
    return rows


def load_file_to_raw_table(conn, settings: Settings, raw_bytes: bytes, filename: str) -> RowLoadResult:
    df = read_validated_file_as_dataframe(raw_bytes, filename)
    total_rows = len(df)

    accepted_df, rejected_df = apply_row_level_rules(df)
    deduped_df, duplicate_dropped = dedup_rows(accepted_df, key_column="iccid")

    loaded_at = _now()

    if not deduped_df.empty:
        insert_rows = build_insert_rows(deduped_df, filename, loaded_at)
        sql = render_sql("queries/insert_raw_actives_rows.sql", DORIS_DATABASE=settings.doris_database)
        executemany_write(conn, sql, insert_rows)

    if not rejected_df.empty:
        insert_rejected_rows(conn, settings, rejected_df, filename, loaded_at)

    rejected_by_reason = (
        rejected_df["reject_reason"].value_counts().to_dict() if not rejected_df.empty else {}
    )

    return RowLoadResult(
        file_name=filename,
        total_rows=total_rows,
        loaded_rows=len(deduped_df),
        rejected_rows=len(rejected_df),
        duplicate_rows_dropped=duplicate_dropped,
        rejected_by_reason=rejected_by_reason,
        status="LOADED",
    )
