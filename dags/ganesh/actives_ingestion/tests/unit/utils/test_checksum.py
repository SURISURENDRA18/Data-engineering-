from src.utils.checksum import compute_sha256


def test_same_content_same_checksum():
    assert compute_sha256(b"hello") == compute_sha256(b"hello")


def test_different_content_different_checksum():
    assert compute_sha256(b"hello") != compute_sha256(b"world")
