from datetime import datetime
from pathlib import Path

import pandas as pd

from src.loaders.raw_loader import read_validated_file_as_dataframe
from src.loaders.rejected_row_loader import insert_rejected_rows

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def test_insert_rejected_rows_writes_expected_columns(fake_doris_conn, test_settings):
    df = read_validated_file_as_dataframe((FIXTURES / "valid_actives_content.csv").read_bytes(), "f.csv")
    rejected_df = df.iloc[[0]].copy()
    rejected_df["reject_reason"] = "NULL_OR_EMPTY_ICCID"

    count = insert_rejected_rows(fake_doris_conn, test_settings, rejected_df, "f.csv", datetime(2026, 1, 1))

    assert count == 1
    row = fake_doris_conn.store.rejected_rows[0]
    assert row["file_name"] == "f.csv"
    assert row["row_number"] == 2  # header=1, first data row=2
    assert row["reject_reason"] == "NULL_OR_EMPTY_ICCID"
    assert "companyid" in row["raw_row_json"]


def test_insert_rejected_rows_noop_on_empty_df(fake_doris_conn, test_settings):
    empty_df = pd.DataFrame()
    count = insert_rejected_rows(fake_doris_conn, test_settings, empty_df, "f.csv", datetime(2026, 1, 1))
    assert count == 0
    assert fake_doris_conn.store.rejected_rows == []
