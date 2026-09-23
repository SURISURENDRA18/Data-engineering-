from src.config.actives_schema import EXPECTED_COLUMNS
from src.validators.header_validator import parse_header, validate_header_columns


def test_correct_header_passes():
    header = ";".join(EXPECTED_COLUMNS)
    fields = parse_header(header, ";")
    assert validate_header_columns(fields).passed is True


def test_missing_column_fails():
    fields = EXPECTED_COLUMNS[:-1]
    outcome = validate_header_columns(fields)
    assert outcome.passed is False
    assert outcome.reason == "invalid_column_count"


def test_reordered_columns_fail():
    fields = list(reversed(EXPECTED_COLUMNS))
    outcome = validate_header_columns(fields)
    assert outcome.passed is False
    assert outcome.reason == "invalid_column_count"


def test_extra_column_fails():
    fields = EXPECTED_COLUMNS + ["UnexpectedExtraColumn"]
    outcome = validate_header_columns(fields)
    assert outcome.passed is False
