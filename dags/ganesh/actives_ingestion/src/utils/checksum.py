"""Content-based file identity for exact-duplicate detection (check 7).

Checksum is computed over the raw bytes as stored in MinIO — i.e. over the
.gz bytes for a compressed file, not its decompressed content. Two files
with identical decompressed content but different gzip encoding are not
treated as duplicates. See README assumptions."""

import hashlib


def compute_sha256(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()
