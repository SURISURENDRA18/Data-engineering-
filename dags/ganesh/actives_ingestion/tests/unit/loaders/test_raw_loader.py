from datetime import datetime
from pathlib import Path

from src.config.actives_schema import RowRejectReason
from src.loaders.raw_loader import (
    RAW_TABLE_LOAD_COLUMNS,
    STREAM_LOAD_COLUMN_SEPARATOR,
    _sql_field,
    apply_row_level_rules,
    build_stream_load_csv,
    dedup_rows,
    load_file_to_raw_table,
    read_validated_file_as_dataframe,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def _valid_bytes() -> bytes:
    return (FIXTURES / "valid_actives_content.csv").read_bytes()


def test_read_validated_file_as_dataframe_renames_and_orders_columns():
    df = read_validated_file_as_dataframe(_valid_bytes(), "HOL_01_Active_000000001_20260921050000.csv")
    assert list(df.columns)[:3] == ["companyid", "companyname", "invoicenumber"]
    assert "iccid" in df.columns
    assert len(df) == 2


def test_apply_row_level_rules_accepts_clean_rows():
    df = read_validated_file_as_dataframe(_valid_bytes(), "f.csv")
    accepted, rejected = apply_row_level_rules(df)
    assert len(accepted) == 2
    assert rejected.empty


def test_apply_row_level_rules_rejects_empty_iccid():
    df = read_validated_file_as_dataframe(_valid_bytes(), "f.csv")
    df.loc[0, "iccid"] = ""
    accepted, rejected = apply_row_level_rules(df)
    assert len(accepted) == 1
    assert len(rejected) == 1
    assert rejected.iloc[0]["reject_reason"] == RowRejectReason.NULL_OR_EMPTY_ICCID


def test_apply_row_level_rules_rejects_empty_customer_number():
    df = read_validated_file_as_dataframe(_valid_bytes(), "f.csv")
    df.loc[0, "customernumber"] = "  "
    _, rejected = apply_row_level_rules(df)
    assert rejected.iloc[0]["reject_reason"] == RowRejectReason.NULL_OR_EMPTY_CUSTOMER_NUMBER


def test_apply_row_level_rules_rejects_zero_company_id():
    df = read_validated_file_as_dataframe(_valid_bytes(), "f.csv")
    df.loc[0, "companyid"] = "0"
    _, rejected = apply_row_level_rules(df)
    assert rejected.iloc[0]["reject_reason"] == RowRejectReason.NULL_OR_ZERO_COMPANY_ID


def test_apply_row_level_rules_checks_iccid_before_other_reasons():
    df = read_validated_file_as_dataframe(_valid_bytes(), "f.csv")
    df.loc[0, "iccid"] = ""
    df.loc[0, "customernumber"] = ""
    df.loc[0, "companyid"] = "0"
    _, rejected = apply_row_level_rules(df)
    assert rejected.iloc[0]["reject_reason"] == RowRejectReason.NULL_OR_EMPTY_ICCID


def test_read_validated_file_as_dataframe_pads_missing_trailing_fields():
    """Real files here routinely omit trailing optional columns entirely
    (no trailing delimiters at all) -- these are implicitly empty, not a
    parse error, matching check 6's row_structure_validator."""
    short_bytes = (FIXTURES / "short_row_content.csv").read_bytes()
    df = read_validated_file_as_dataframe(short_bytes, "f.csv")
    assert len(df) == 2
    assert len(df.columns) == 46
    # the short row's missing trailing fields must come through as "", not raise/misalign
    assert (df.iloc[1] == "").any()


def test_dedup_rows_keeps_last_occurrence():
    df = read_validated_file_as_dataframe(_valid_bytes(), "f.csv")
    df.loc[1, "iccid"] = df.loc[0, "iccid"]  # force a duplicate iccid
    df.loc[1, "companyname"] = "LATEST"

    deduped, dropped = dedup_rows(df, key_column="iccid")

    assert dropped == 1
    assert len(deduped) == 1
    assert deduped.iloc[0]["companyname"] == "LATEST"


def test_load_file_to_raw_table_end_to_end(fake_doris_conn, test_settings, fake_stream_load):
    result = load_file_to_raw_table(fake_doris_conn, test_settings, _valid_bytes(), "f.csv", stream_load_fn=fake_stream_load)

    assert result.total_rows == 2
    assert result.loaded_rows == 2
    assert result.rejected_rows == 0
    assert len(fake_doris_conn.store.raw_rows) == 2


def test_load_file_to_raw_table_routes_rejects_to_rejected_table(fake_doris_conn, test_settings, fake_stream_load):
    df = read_validated_file_as_dataframe(_valid_bytes(), "f.csv")
    df.loc[0, "iccid"] = ""
    # Round-trip through CSV bytes so load_file_to_raw_table parses it itself.
    raw_bytes = _rebuild_source_csv(df)
    result = load_file_to_raw_table(fake_doris_conn, test_settings, raw_bytes, "f.csv", stream_load_fn=fake_stream_load)

    assert result.loaded_rows == 1
    assert result.rejected_rows == 1
    assert len(fake_doris_conn.store.rejected_rows) == 1
    assert fake_doris_conn.store.rejected_rows[0]["reject_reason"] == RowRejectReason.NULL_OR_EMPTY_ICCID


def test_sql_field_converts_empty_non_iccid_value_to_null_token():
    """Per mentor guidance: every raw_actives_daily column is nullable
    except iccid (its NOT NULL key) -- an empty value in any other column
    must load as a real Doris NULL (the CSV `\\N` token), not the string
    "" (which is a distinct, non-NULL value)."""
    assert _sql_field("companyname", "") == "\\N"
    assert _sql_field("companyname", "   ") == "\\N"
    assert _sql_field("companyname", "Tata MVNO") == "Tata MVNO"


def test_sql_field_never_nulls_iccid():
    # iccid is NOT NULL -- rows with an empty iccid are rejected upstream
    # (apply_row_level_rules) before this ever runs, but this must never
    # substitute \N for it regardless.
    assert _sql_field("iccid", "") == ""


def test_build_stream_load_csv_nulls_empty_fields_except_iccid():
    df = read_validated_file_as_dataframe(_valid_bytes(), "f.csv")
    df.loc[0, "departmentname"] = ""

    csv_body = build_stream_load_csv(df, "f.csv", datetime(2026, 1, 1, 12, 0, 0))
    first_line = csv_body.decode("utf-8").split("\n")[0]
    row = dict(zip(RAW_TABLE_LOAD_COLUMNS, first_line.split(STREAM_LOAD_COLUMN_SEPARATOR)))

    assert row["departmentname"] == "\\N"
    assert row["iccid"] not in ("", "\\N")
    assert row["companyname"] not in ("", "\\N")  # non-empty fields pass through untouched


def _rebuild_source_csv(df) -> bytes:
    """Rebuilds source-format (original header names, ';'-delimited) bytes
    from a DB-column-named dataframe, so load_file_to_raw_table's own CSV
    parsing is exercised rather than bypassed."""
    from src.config.actives_schema import COLUMN_DB_MAP

    db_to_source = {v: k for k, v in COLUMN_DB_MAP.items()}
    source_df = df.rename(columns=db_to_source)
    from src.config.actives_schema import EXPECTED_COLUMNS

    source_df = source_df[EXPECTED_COLUMNS]
    return source_df.to_csv(sep=";", index=False).encode("utf-8")
