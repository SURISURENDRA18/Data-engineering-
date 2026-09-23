from src.validators.sequence_validator import validate_sequence


def test_first_sequence_for_region_passes():
    assert validate_sequence("HOL", 1, None).passed is True


def test_increasing_sequence_passes():
    assert validate_sequence("HOL", 5, 4).passed is True


def test_repeated_sequence_fails():
    outcome = validate_sequence("HOL", 3, 3)
    assert outcome.passed is False
    assert outcome.reason == "sequence_invalid"


def test_out_of_order_sequence_fails():
    outcome = validate_sequence("HOL", 2, 5)
    assert outcome.passed is False
    assert outcome.reason == "sequence_invalid"
