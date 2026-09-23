"""Pure string helpers for building MinIO destination keys. No I/O."""

import posixpath


def build_validated_key(landing_key: str, landing_prefix: str, validated_prefix: str) -> str:
    relative = _relative_name(landing_key, landing_prefix)
    return posixpath.join(validated_prefix, relative)


def build_rejected_key(
    landing_key: str, landing_prefix: str, rejected_prefix_for_reason: str
) -> str:
    relative = _relative_name(landing_key, landing_prefix)
    return posixpath.join(rejected_prefix_for_reason, relative)


def _relative_name(key: str, prefix: str) -> str:
    return key[len(prefix):] if key.startswith(prefix) else posixpath.basename(key)


def is_gzip_filename(filename: str) -> bool:
    return filename.lower().endswith(".gz")
