"""Writes row-level rejects (Task 2 rule 6) to the audit table."""

import json
from datetime import datetime

import pandas as pd

from src.clients.doris_client import executemany_write, render_sql
from src.config.actives_schema import RAW_TABLE_DATA_COLUMNS
from src.config.settings import Settings


def serialize_row_json(row: pd.Series) -> str:
    return json.dumps({col: row[col] for col in RAW_TABLE_DATA_COLUMNS}, ensure_ascii=False)


def insert_rejected_rows(conn, settings: Settings, rejected_df: pd.DataFrame, file_name: str, rejected_at: datetime) -> int:
    if rejected_df.empty:
        return 0

    rows = []
    for original_index, row in rejected_df.iterrows():
        row_number = original_index + 2  # 1 = header, 2 = first data row
        rows.append(
            (
                file_name,
                row_number,
                row["iccid"],
                row["customernumber"],
                row["companyid"],
                row["reject_reason"],
                serialize_row_json(row),
                rejected_at,
            )
        )

    sql = render_sql("queries/insert_rejected_rows.sql", DORIS_DATABASE=settings.doris_database)
    return executemany_write(conn, sql, rows)
