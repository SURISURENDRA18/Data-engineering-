"""Unit tests for RowValidator."""

from actives_ingestion.constants import (
    ROW_REASON_NULL_OR_EMPTY_CUSTOMER_NUMBER,
    ROW_REASON_NULL_OR_EMPTY_ICCID,
    ROW_REASON_NULL_OR_ZERO_COMPANY_ID,
)
from actives_ingestion.validators import RowValidator


def test_valid_row():
    row = {
        "CompanyID": "100",
        "CustomerNumber": "CUST-001",
        "ICCID": "89014103211118510720",
    }
    result = RowValidator.validate_row(row, row_number=1, raw_row="100,CUST-001,89014103211118510720")
    assert result.is_valid is True
    assert result.reason is None


def test_empty_iccid():
    row = {
        "CompanyID": "100",
        "CustomerNumber": "CUST-001",
        "ICCID": "",
    }
    result = RowValidator.validate_row(row, row_number=1)
    assert result.is_valid is False
    assert result.reason == ROW_REASON_NULL_OR_EMPTY_ICCID

    row["ICCID"] = "   "
    result = RowValidator.validate_row(row, row_number=1)
    assert result.is_valid is False
    assert result.reason == ROW_REASON_NULL_OR_EMPTY_ICCID


def test_empty_customer_number():
    row = {
        "CompanyID": "100",
        "CustomerNumber": "",
        "ICCID": "89014103211118510720",
    }
    result = RowValidator.validate_row(row, row_number=2)
    assert result.is_valid is False
    assert result.reason == ROW_REASON_NULL_OR_EMPTY_CUSTOMER_NUMBER

    row["CustomerNumber"] = "   "
    result = RowValidator.validate_row(row, row_number=2)
    assert result.is_valid is False
    assert result.reason == ROW_REASON_NULL_OR_EMPTY_CUSTOMER_NUMBER


def test_empty_company_id():
    row = {
        "CompanyID": "",
        "CustomerNumber": "CUST-001",
        "ICCID": "89014103211118510720",
    }
    result = RowValidator.validate_row(row, row_number=3)
    assert result.is_valid is False
    assert result.reason == ROW_REASON_NULL_OR_ZERO_COMPANY_ID


def test_zero_company_id():
    row = {
        "CompanyID": "0",
        "CustomerNumber": "CUST-001",
        "ICCID": "89014103211118510720",
    }
    result = RowValidator.validate_row(row, row_number=4)
    assert result.is_valid is False
    assert result.reason == ROW_REASON_NULL_OR_ZERO_COMPANY_ID

    row["CompanyID"] = "00"
    result = RowValidator.validate_row(row, row_number=5)
    assert result.is_valid is False
    assert result.reason == ROW_REASON_NULL_OR_ZERO_COMPANY_ID
