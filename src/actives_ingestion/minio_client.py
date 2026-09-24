"""MinIO client wrapper for safe and optimized object storage operations."""

import io
import os
import time
from dataclasses import dataclass
from typing import Any

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

from actives_ingestion.config import (
    MinioConfig,
    get_airflow_connection_details,
    pipeline_config,
)
from actives_ingestion.utils import get_logger

logger = get_logger("minio_client")


@dataclass
class MinioObjectMeta:
    """Metadata extracted during MinIO object listing."""
    object_name: str
    size: int
    etag: str = ""
    last_modified: Any = None


class MinioService:
    """Provides safe object storage operations with non-destructive routing."""

    def __init__(self, config: MinioConfig | None = None, client: Minio | None = None, in_memory: bool = False):
        self.config = config or pipeline_config.minio
        self.in_memory = in_memory
        self._client = client
        self._bucket_verified: bool = False
        self._size_cache: dict[str, int] = {}

        # In-memory storage for test fixtures and offline execution
        self._mem_buckets: dict[str, dict[str, bytes]] = {
            self.config.bucket: {}
        }

    def _get_client(self) -> Minio:
        """Instantiate Minio client if not provided."""
        if self._client is None:
            connection = get_airflow_connection_details("minio_default")
            endpoint = self.config.endpoint
            access_key = self.config.access_key
            secret_key = self.config.secret_key
            secure = self.config.secure
            if connection:
                endpoint = str(connection["endpoint"])
                access_key = str(connection["login"])
                if connection.get("password"):
                    secret_key = str(connection["password"])
                extra = connection.get("extra") or {}
                secure = bool(extra.get("secure", secure))
            self._client = Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
            )
        return self._client

    def ensure_bucket_exists(self) -> None:
        """Create bucket if it does not exist, avoiding repeated checks."""
        if self.in_memory or self._bucket_verified:
            return
        try:
            client = self._get_client()
            if not client.bucket_exists(self.config.bucket):
                client.make_bucket(self.config.bucket)
            self._bucket_verified = True
        except S3Error as e:
            # Existing production buckets may intentionally deny
            # GetBucketLocation/HEAD while still allowing object listing and
            # reads. Bucket creation is not part of a pipeline run.
            if e.code in {"AccessDenied", "AllAccessDisabled"}:
                logger.warning(
                    "Bucket verification is not permitted for '%s'; continuing and validating access by object listing.",
                    self.config.bucket,
                )
                self._bucket_verified = True
                return
            logger.error("Could not verify or create bucket %s: %s", self.config.bucket, e)
            raise
        except Exception as e:
            logger.error("Could not verify or create bucket %s: %s", self.config.bucket, e)
            raise

    def list_landing_objects(self) -> list[MinioObjectMeta]:
        """List all landing objects with metadata populated directly from listing."""
        return self._list_objects_with_meta(self.config.landing_prefix)

    def list_landing_files(self) -> list[str]:
        """List all object keys under landing prefix."""
        return [obj.object_name for obj in self.list_landing_objects()]

    def list_validated_objects(self) -> list[MinioObjectMeta]:
        """List all validated objects with metadata populated directly from listing."""
        return self._list_objects_with_meta(self.config.validated_prefix)

    def list_validated_files(self) -> list[str]:
        """List all object keys under validated prefix."""
        return [obj.object_name for obj in self.list_validated_objects()]

    def list_rejected_objects(self) -> list[MinioObjectMeta]:
        """List all rejected objects with metadata populated directly from listing."""
        return self._list_objects_with_meta(self.config.rejected_prefix)

    def list_rejected_files(self) -> list[str]:
        """List all object keys under rejected prefix."""
        return [obj.object_name for obj in self.list_rejected_objects()]

    def object_exists(self, object_name: str) -> bool:
        """Check whether an object exists in the configured MinIO bucket."""
        if self.in_memory:
            bucket_data = self._mem_buckets.get(self.config.bucket, {})
            return object_name in bucket_data

        try:
            client = self._get_client()
            client.stat_object(self.config.bucket, object_name)
            return True
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchObject", "404 Not Found"):
                return False
            raise
        except Exception:
            return False

    def get_destination_files(self) -> dict[str, str]:
        """Return mapping of basename -> destination object key for all validated and rejected files."""
        destinations: dict[str, str] = {}
        for obj in self.list_validated_objects():
            destinations[os.path.basename(obj.object_name)] = obj.object_name
        for obj in self.list_rejected_objects():
            destinations[os.path.basename(obj.object_name)] = obj.object_name
        return destinations

    def destination_exists(self, filename: str) -> tuple[bool, str | None]:
        """Check if a filename already exists under validated or rejected destinations."""
        destinations = self.get_destination_files()
        if filename in destinations:
            return True, destinations[filename]
        return False, None

    def _list_prefix(self, prefix: str) -> list[str]:
        """List object keys under given prefix (backwards-compatible alias)."""
        return [obj.object_name for obj in self._list_objects_with_meta(prefix)]

    def _list_objects_with_meta(self, prefix: str) -> list[MinioObjectMeta]:
        """List objects under given prefix and cache their sizes to avoid subsequent HEAD calls."""
        if self.in_memory:
            bucket_data = self._mem_buckets.get(self.config.bucket, {})
            results = [
                MinioObjectMeta(object_name=key, size=len(data))
                for key, data in sorted(bucket_data.items())
                if key.startswith(prefix) and key != prefix and not key.endswith("/")
            ]
            for r in results:
                self._size_cache[r.object_name] = r.size
            return results

        try:
            client = self._get_client()
            objects = client.list_objects(self.config.bucket, prefix=prefix, recursive=True)
            results = []
            for obj in objects:
                # Exclude directory placeholders
                if not obj.is_dir and not obj.object_name.endswith("/"):
                    size = obj.size if obj.size is not None else 0
                    meta = MinioObjectMeta(
                        object_name=obj.object_name,
                        size=size,
                        etag=getattr(obj, "etag", "") or "",
                        last_modified=getattr(obj, "last_modified", None),
                    )
                    results.append(meta)
                    self._size_cache[obj.object_name] = size
            results.sort(key=lambda x: x.object_name)
            return results
        except Exception as e:
            logger.error("Failed to list objects with prefix '%s': %s", prefix, e)
            raise

    def get_object_stream(self, object_name: str) -> io.BytesIO:
        """Retrieve object contents as a seekable BytesIO stream."""
        if self.in_memory:
            bucket_data = self._mem_buckets.get(self.config.bucket, {})
            if object_name not in bucket_data:
                raise FileNotFoundError(f"Object {object_name} not found in memory bucket")
            return io.BytesIO(bucket_data[object_name])

        client = self._get_client()
        response = None
        try:
            response = client.get_object(self.config.bucket, object_name)
            data = response.read()
            return io.BytesIO(data)
        finally:
            if response:
                response.close()
                response.release_conn()

    def get_object_size(self, object_name: str) -> int:
        """Retrieve object size in bytes, using cached listing metadata whenever possible."""
        if object_name in self._size_cache:
            return self._size_cache[object_name]

        if self.in_memory:
            bucket_data = self._mem_buckets.get(self.config.bucket, {})
            size = len(bucket_data.get(object_name, b""))
            self._size_cache[object_name] = size
            return size

        client = self._get_client()
        stat = client.stat_object(self.config.bucket, object_name)
        self._size_cache[object_name] = stat.size
        return stat.size

    def put_object_data(self, object_name: str, data: bytes) -> None:
        """Upload byte data to bucket (used for test setup and verification)."""
        self._size_cache[object_name] = len(data)
        if self.in_memory:
            self._mem_buckets.setdefault(self.config.bucket, {})[object_name] = data
            return

        client = self._get_client()
        stream = io.BytesIO(data)
        client.put_object(
            bucket_name=self.config.bucket,
            object_name=object_name,
            data=stream,
            length=len(data),
        )

    def copy_object(self, source_object: str, destination_object: str) -> None:
        """Copy an object to a destination without deleting the source."""
        if self.in_memory:
            bucket_data = self._mem_buckets.setdefault(self.config.bucket, {})
            if source_object in bucket_data:
                bucket_data[destination_object] = bucket_data[source_object]
                if source_object in self._size_cache:
                    self._size_cache[destination_object] = self._size_cache[source_object]
            return

        client = self._get_client()
        copy_source = CopySource(self.config.bucket, source_object)

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                client.copy_object(self.config.bucket, destination_object, copy_source)
                if source_object in self._size_cache:
                    self._size_cache[destination_object] = self._size_cache[source_object]
                return
            except Exception as exc:
                if attempt == max_attempts:
                    logger.error("Failed to copy %s -> %s after %d attempts: %s", source_object, destination_object, max_attempts, exc)
                    raise
                time.sleep(0.2 * (2 ** (attempt - 1)))

    def move_object(self, source_object: str, destination_object: str) -> None:
        """Backward-compatible alias for non-destructive copy routing."""
        self.copy_object(source_object, destination_object)

    def move_to_validated(self, source_object: str) -> str:
        """Copy object from landing to validated folder, retaining the source."""
        file_name = os.path.basename(source_object)
        destination = f"{self.config.validated_prefix.rstrip('/')}/{file_name}"
        self.move_object(source_object, destination)
        logger.debug("Moved %s -> %s", source_object, destination)
        return destination

    def move_to_rejected(self, source_object: str, reason: str) -> str:
        """Copy object from landing to rejected/<reason>, retaining the source."""
        file_name = os.path.basename(source_object)
        destination = f"{self.config.rejected_prefix.rstrip('/')}/{reason}/{file_name}"
        self.move_object(source_object, destination)
        logger.debug("Moved %s -> %s (reason: %s)", source_object, destination, reason)
        return destination
