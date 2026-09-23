"""Thin wrapper around the `minio` Python SDK. This is the only module
that touches the MinIO Airflow Connection directly; everything above it
works with a plain Minio client instance, so tests can inject a fake one
instead. Matches the client library already used across this repo's
other DAGs (task1_etl_dag.py, task2_warehouse_dag.py, etc.).
"""

import os
import posixpath
from typing import List, Set, TypedDict

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error


class ObjectSummary(TypedDict):
    key: str
    size: int


def get_minio_client_from_conn(conn_id: str) -> Minio:
    """Builds a Minio client from an Airflow Connection (preferred) or,
    if Airflow / the connection isn't available (local/unit-test runs),
    from MINIO_* environment variables."""
    host = access_key = secret_key = None

    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        host = f"{conn.host}:{conn.port}"
        access_key = conn.login
        secret_key = conn.password
    except Exception:
        host = os.environ.get("MINIO_HOST")
        access_key = os.environ.get("MINIO_ACCESS_KEY")
        secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not host:
        raise RuntimeError(
            f"No MinIO connection available for conn_id={conn_id!r} and no "
            "MINIO_HOST fallback set."
        )

    return Minio(host, access_key=access_key, secret_key=secret_key, secure=False)


def list_objects(client: Minio, bucket: str, prefix: str) -> List[ObjectSummary]:
    """Returns every object under ``prefix``."""
    objects: List[ObjectSummary] = []
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if obj.is_dir:
            continue
        objects.append(ObjectSummary(key=obj.object_name, size=obj.size))
    return objects


def list_processed_filenames(client: Minio, bucket: str, validated_prefix: str, rejected_prefix: str) -> Set[str]:
    """Filenames already present under validated/ or any rejected/<reason>/
    -- since landing/ is never cleared out (see route_file), this is what
    idempotency checks against instead of "is it still in landing/"."""
    names: Set[str] = set()
    for prefix in (validated_prefix, rejected_prefix):
        for obj in list_objects(client, bucket, prefix):
            names.add(posixpath.basename(obj["key"]))
    return names


def read_object_bytes(client: Minio, bucket: str, key: str) -> bytes:
    response = client.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def object_exists(client: Minio, bucket: str, key: str) -> bool:
    try:
        client.stat_object(bucket, key)
        return True
    except S3Error as exc:
        if exc.code in ("NoSuchKey", "NoSuchObject"):
            return False
        raise


def route_file(client: Minio, bucket: str, source_key: str, dest_key: str) -> None:
    """Copies a file to validated/ or rejected/<reason>/ WITHOUT deleting it
    from landing/. landing/ is a shared pool other candidates' pipelines
    also read from in this environment, so this must be non-destructive --
    idempotent to call again (overwriting an identical destination is a
    no-op in effect)."""
    client.copy_object(bucket, dest_key, CopySource(bucket, source_key))
