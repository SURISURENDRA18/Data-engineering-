"""Utility functions for Actives Ingestion Pipeline."""

import hashlib
import io
import logging
from collections.abc import Generator
from typing import Any

logger = logging.getLogger("actives_ingestion")


def get_logger(name: str = "actives_ingestion") -> logging.Logger:
    """Return a configured logger with consistent formatting."""
    log = logging.getLogger(name)
    if not log.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        log.addHandler(handler)
        log.setLevel(logging.INFO)
    return log


def calculate_sha256(stream: io.BufferedIOBase, chunk_size: int = 65536) -> str:
    """Calculate SHA-256 hex digest of a stream without loading entire file into memory.

    Resets the stream position to start after hashing if stream is seekable.
    """
    hasher = hashlib.sha256()
    while chunk := stream.read(chunk_size):
        hasher.update(chunk)
    if stream.seekable():
        stream.seek(0)
    return hasher.hexdigest()


def chunk_list(items: list[Any], chunk_size: int) -> Generator[list[Any], None, None]:
    """Yield successive chunks of items with size chunk_size."""
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def is_gzip_bytes(header: bytes) -> bool:
    """Check if first two bytes match gzip magic number (0x1f, 0x8b)."""
    return len(header) >= 2 and header[:2] == b"\x1f\x8b"
