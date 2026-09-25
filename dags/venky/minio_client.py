from __future__ import annotations

from airflow.hooks.base import BaseHook
from minio import Minio
from minio.commonconfig import CopySource

from .config import MINIO_CONN_ID


def get_minio_client() -> Minio:
    """
    Build a MinIO client from an Airflow Connection.

    Airflow Connection:
        actives_minio
    """

    conn = BaseHook.get_connection(MINIO_CONN_ID)

    host = conn.host
    port = conn.port or 9000

    endpoint = f"{host}:{port}"

    extra = conn.extra_dejson

    secure = extra.get("secure", False)

    return Minio(
        endpoint,
        access_key=conn.login,
        secret_key=conn.password,
        secure=secure,
    )


def list_files(client: Minio, bucket: str, prefix: str):
    """
    Return files under a MinIO prefix.
    """

    return list(
        client.list_objects(
            bucket,
            prefix=prefix,
            recursive=True,
        )
    )


def move_object(
    client: Minio,
    bucket: str,
    source: str,
    destination: str,
) -> None:
    """
    Move an object by copying it.
    # Move an object by copying it and then deleting the original.
    """

    client.copy_object(
        bucket,
        destination,
        CopySource(bucket, source),
    )

    # client.remove_object(bucket, source)


def download_object(
    client: Minio,
    bucket: str,
    object_name: str,
    local_path: str,
) -> None:
    """
    Download a MinIO object to local worker storage.
    """

    client.fget_object(
        bucket,
        object_name,
        local_path,
    )