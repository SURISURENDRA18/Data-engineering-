from src.validators.row_structure_validator import count_data_rows, pad_row, validate_row_field_counts

HEADER = "a;b;c"


def test_matching_row_counts_pass():
    text = f"{HEADER}\n1;2;3\n4;5;6\n"
    assert validate_row_field_counts(text, ";", 3).passed is True


def test_row_with_missing_trailing_fields_passes():
    """A missing trailing field is implicitly empty, same as two
    consecutive delimiters -- real files here routinely omit trailing
    optional columns entirely rather than emitting them as empty."""
    text = f"{HEADER}\n1;2;3\n4;5\n"  # third field missing entirely, no trailing delimiter
    outcome = validate_row_field_counts(text, ";", 3)
    assert outcome.passed is True


def test_row_with_too_many_fields_fails():
    text = f"{HEADER}\n1;2;3\n4;5;6;7\n"
    outcome = validate_row_field_counts(text, ";", 3)
    assert outcome.passed is False
    assert outcome.reason == "invalid_column_count"
    assert "row 3" in outcome.detail


def test_row_with_explicit_empty_field_passes():
    text = f"{HEADER}\n1;2;3\n4;;6\n"  # consecutive delimiters -- explicit empty field
    assert validate_row_field_counts(text, ";", 3).passed is True


def test_pad_row_fills_missing_trailing_fields():
    assert pad_row(["1", "2"], 3) == ["1", "2", ""]


def test_pad_row_leaves_full_row_unchanged():
    assert pad_row(["1", "2", "3"], 3) == ["1", "2", "3"]


def test_pad_row_truncates_overlong_row():
    assert pad_row(["1", "2", "3", "4"], 3) == ["1", "2", "3"]


def test_count_data_rows_excludes_header():
    text = f"{HEADER}\n1;2;3\n4;5;6\n7;8;9\n"
    assert count_data_rows(text, ";") == 3
