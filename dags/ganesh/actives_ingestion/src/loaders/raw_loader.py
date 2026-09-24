"""Task 2 per-file processing: parse a validated file into rows, apply the
row-level rejection rules, deduplicate on iccid, and insert into the raw
table. Tracking-table I/O (load-status idempotency, marking LOADED /
LOAD_FAILED) is the caller's (raw_load_pipeline's) responsibility, so this
module stays a pure "given file bytes, produce rows" transformer that
either succeeds or raises.
"""

import csv
import io
from datetime import datetime, timezone
from typing import Callable, List, Optional, Tuple

import pandas as pd

from src.clients.doris_client import get_doris_connection_params, render_sql, run_ddl_script, stream_load
from src.config.actives_schema import COLUMN_DB_MAP, CSV_DELIMITER, EXPECTED_COLUMNS, RAW_TABLE_DATA_COLUMNS, RowRejectReason
from src.config.settings import Settings
from src.loaders.rejected_row_loader import insert_rejected_rows
from src.models.validation_result import RowLoadResult
from src.utils.checksum import compute_sha256
from src.validators.file_content_validator import decompress_if_gzip
from src.validators.row_structure_validator import pad_row

# Doris Stream Load splits the CSV body on this literal byte. A control
# character (not ';'/',') is used deliberately -- several source columns
# (e.g. CompanyName, whitelistedzones) are free text and could plausibly
# contain a printable delimiter, but not a raw 0x01 byte.
STREAM_LOAD_COLUMN_SEPARATOR = "\x01"
RAW_TABLE_LOAD_COLUMNS = ["file_name"] + RAW_TABLE_DATA_COLUMNS + ["loaded_at"]

StreamLoadFn = Callable[[Settings, bytes, str], dict]


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_raw_tables_exist(conn, settings: Settings) -> None:
    for ddl_file in (
        "ddl/001_create_raw_actives_daily.sql",
        "ddl/002_create_raw_actives_daily_rejected.sql",
    ):
        run_ddl_script(conn, render_sql(ddl_file, DORIS_DATABASE=settings.doris_database))


def read_validated_file_as_dataframe(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parses via csv.reader + explicit padding, not pd.read_csv directly --
    real files here routinely omit trailing optional columns entirely
    (missing delimiters, not just empty values), which pandas' own C
    tokenizer rejects outright. A missing trailing field is treated as
    implicitly empty, matching check 6's row_structure_validator."""
    content_bytes = decompress_if_gzip(raw_bytes, filename)
    text = content_bytes.decode("utf-8")

    reader = csv.reader(io.StringIO(text), delimiter=CSV_DELIMITER)
    next(reader, None)  # header already validated by check 5, not re-read here
    expected_count = len(EXPECTED_COLUMNS)
    rows = [pad_row(row, expected_count) for row in reader if row]

    df = pd.DataFrame(rows, columns=EXPECTED_COLUMNS, dtype=str)
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


def _sql_field(column: str, value) -> str:
    """Every raw_actives_daily column is nullable except iccid (its NOT
    NULL UNIQUE KEY) -- an empty source value in any other column must
    load as a real NULL, not the string "". Doris's CSV Stream Load
    format uses the literal `\\N` token for NULL, same convention as
    MySQL LOAD DATA. iccid is never empty here: apply_row_level_rules
    already routed empty-iccid rows to raw_actives_daily_rejected before
    this function runs."""
    text = str(value)
    if column != "iccid" and text.strip() == "":
        return "\\N"
    return text


def build_stream_load_csv(df: pd.DataFrame, file_name: str, loaded_at: datetime) -> bytes:
    """Serializes accepted rows into Doris Stream Load's raw CSV body --
    one line per row, columns joined by STREAM_LOAD_COLUMN_SEPARATOR, in
    the exact order RAW_TABLE_LOAD_COLUMNS/the `columns` param declares."""
    loaded_at_str = loaded_at.strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    for _, row in df.iterrows():
        values = [file_name] + [_sql_field(col, row[col]) for col in RAW_TABLE_DATA_COLUMNS] + [loaded_at_str]
        lines.append(STREAM_LOAD_COLUMN_SEPARATOR.join(values))
    return "\n".join(lines).encode("utf-8")


def _default_stream_load(settings: Settings, csv_body: bytes, label: str) -> dict:
    """Production Stream Load call: resolves host/credentials from the same
    Airflow Connection the PyMySQL path uses, and sends to the configurable
    Stream Load HTTP port (separate from the MySQL-protocol query port)."""
    params = get_doris_connection_params(settings.doris_conn_id)
    return stream_load(
        host=params["host"],
        http_port=settings.doris_stream_load_port,
        user=params["user"],
        password=params["password"],
        database=settings.doris_database,
        table=settings.raw_table,
        csv_body=csv_body,
        columns=RAW_TABLE_LOAD_COLUMNS,
        label=label,
        column_separator=STREAM_LOAD_COLUMN_SEPARATOR,
    )


def load_file_to_raw_table(
    conn, settings: Settings, raw_bytes: bytes, filename: str, stream_load_fn: Optional[StreamLoadFn] = None
) -> RowLoadResult:
    stream_load_fn = stream_load_fn or _default_stream_load

    df = read_validated_file_as_dataframe(raw_bytes, filename)
    total_rows = len(df)

    accepted_df, rejected_df = apply_row_level_rules(df)
    deduped_df, duplicate_dropped = dedup_rows(accepted_df, key_column="iccid")

    loaded_at = _now()

    if not deduped_df.empty:
        csv_body = build_stream_load_csv(deduped_df, filename, loaded_at)
        # Deterministic per source-file content: a retried Stream Load call
        # for the same file/bytes reuses the same label, which Doris treats
        # as an idempotent no-op ("Label Already Exists") rather than a
        # second load.
        label = f"ganesh_raw_{compute_sha256(raw_bytes)}"
        stream_load_fn(settings, csv_body, label)

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
