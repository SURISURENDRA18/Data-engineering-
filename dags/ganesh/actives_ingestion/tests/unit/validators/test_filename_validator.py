import pytest

from src.validators.filename_validator import extract_region_and_sequence, validate_filename


@pytest.mark.parametrize(
    "filename",
    [
        "HOL_01_Active_000123456_20260921050000.csv",
        "SNG_01_Actives_000123457_20260921053000.csv.gz",
        "HOL_1_Active_1_20260921050000.csv",
    ],
)
def test_valid_filenames_pass(filename):
    assert validate_filename(filename).passed is True


def test_regex_does_not_enforce_exact_14_digit_timestamp():
    """The assessment's prose says 'a 14 digit timestamp', but the regex it
    gives us to use verbatim (`[0-9]+` for that field) doesn't actually
    enforce a length of 14. Per the master prompt's instruction to use the
    exact given pattern rather than a stricter reading of the prose, a
    shorter/longer numeric timestamp still passes check 1. Documented as
    an explicit assumption in the README."""
    assert validate_filename("HOL_01_Active_000123456_2026092105.csv").passed is True


@pytest.mark.parametrize(
    "filename",
    [
        "HOL_01_Activations_000123456_20260921050000.csv",  # wrong feed name
        "hol_01_active_000123456_20260921050000.csv",  # wrong case
        "HOL01_Active_000123456_20260921050000.csv",  # missing underscore
        "HOL_01_Active_000123456_20260921050000.txt",  # wrong extension
        "HOL_01_Active_000123456_20260921050000.csv.zip",  # wrong compression
        "HOL_A1_Active_000123456_20260921050000.csv",  # non-numeric instance
    ],
)
def test_invalid_filenames_fail_with_pattern_mismatch(filename):
    outcome = validate_filename(filename)
    assert outcome.passed is False
    assert outcome.reason == "pattern_mismatch"


def test_extract_region_and_sequence():
    region, sequence = extract_region_and_sequence("HOL_01_Active_000123456_20260921050000.csv")
    assert region == "HOL"
    assert sequence == 123456
