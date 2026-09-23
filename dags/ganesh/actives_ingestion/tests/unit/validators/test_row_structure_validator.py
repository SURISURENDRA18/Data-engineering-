from src.validators.row_structure_validator import count_data_rows, validate_row_field_counts

HEADER = "a;b;c"


def test_matching_row_counts_pass():
    text = f"{HEADER}\n1;2;3\n4;5;6\n"
    assert validate_row_field_counts(text, ";", 3).passed is True


def test_mismatched_row_count_fails():
    text = f"{HEADER}\n1;2;3\n4;5\n"
    outcome = validate_row_field_counts(text, ";", 3)
    assert outcome.passed is False
    assert outcome.reason == "invalid_column_count"
    assert "row 3" in outcome.detail


def test_count_data_rows_excludes_header():
    text = f"{HEADER}\n1;2;3\n4;5;6\n7;8;9\n"
    assert count_data_rows(text, ";") == 3
