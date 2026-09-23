"""MinIO client wrapper for safe object storage operations."""

import io
import os

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

from actives_ingestion.config import MinioConfig, get_airflow_connection_details, pipeline_config
from actives_ingestion.utils import get_logger

logger = get_logger("minio_client")


class MinioService:
    """Provides safe MinIO object storage operations: listing, streaming, and copy-delete movements."""

    def __init__(self, config: MinioConfig | None = None, client: Minio | None = None, in_memory: bool = False):
        self.config = config or pipeline_config.minio
        self.in_memory = in_memory
        self._client = client

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
        """Create bucket if it does not exist."""
        if self.in_memory:
            return
        try:
            client = self._get_client()
            if not client.bucket_exists(self.config.bucket):
                client.make_bucket(self.config.bucket)
        except S3Error as e:
            # Existing production buckets may intentionally deny
            # GetBucketLocation/HEAD while still allowing object listing and
            # reads. Bucket creation is not part of a pipeline run.
            if e.code in {"AccessDenied", "AllAccessDisabled"}:
                logger.warning(
                    "Bucket verification is not permitted for '%s'; continuing and validating access by object listing.",
                    self.config.bucket,
                )
                return
            logger.error("Could not verify or create bucket %s: %s", self.config.bucket, e)
            raise
        except Exception as e:
            logger.error("Could not verify or create bucket %s: %s", self.config.bucket, e)
            raise

    def list_landing_files(self) -> list[str]:
        """List all object keys under landing prefix."""
        return self._list_prefix(self.config.landing_prefix)

    def list_validated_files(self) -> list[str]:
        """List all object keys under validated prefix."""
        return self._list_prefix(self.config.validated_prefix)

    def _list_prefix(self, prefix: str) -> list[str]:
        """List object keys under given prefix."""
        if self.in_memory:
            bucket_data = self._mem_buckets.get(self.config.bucket, {})
            return [
                key for key in sorted(bucket_data.keys())
                if key.startswith(prefix) and key != prefix and not key.endswith("/")
            ]

        try:
            client = self._get_client()
            objects = client.list_objects(self.config.bucket, prefix=prefix, recursive=True)
            results = []
            for obj in objects:
                # Exclude directory placeholders
                if not obj.is_dir and not obj.object_name.endswith("/"):
                    results.append(obj.object_name)
            return sorted(results)
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
        """Retrieve object size in bytes."""
        if self.in_memory:
            bucket_data = self._mem_buckets.get(self.config.bucket, {})
            return len(bucket_data.get(object_name, b""))

        client = self._get_client()
        stat = client.stat_object(self.config.bucket, object_name)
        return stat.size

    def put_object_data(self, object_name: str, data: bytes) -> None:
        """Upload byte data to bucket (used for test setup and verification)."""
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

    def move_object(self, source_object: str, destination_object: str) -> None:
        """Safely move an object from source to destination (copy-delete pattern)."""
        if self.in_memory:
            bucket_data = self._mem_buckets.setdefault(self.config.bucket, {})
            if source_object in bucket_data:
                bucket_data[destination_object] = bucket_data.pop(source_object)
            return

        client = self._get_client()
        copy_source = CopySource(self.config.bucket, source_object)
        client.copy_object(self.config.bucket, destination_object, copy_source)
        client.stat_object(self.config.bucket, destination_object)
        client.remove_object(self.config.bucket, source_object)

    def move_to_validated(self, source_object: str) -> str:
        """Move object from landing to validated folder."""
        file_name = os.path.basename(source_object)
        destination = f"{self.config.validated_prefix.rstrip('/')}/{file_name}"
        self.move_object(source_object, destination)
        logger.info(f"Moved {source_object} -> {destination}")
        return destination

    def move_to_rejected(self, source_object: str, reason: str) -> str:
        """Move object from landing to rejected/<reason>/ folder."""
        file_name = os.path.basename(source_object)
        destination = f"{self.config.rejected_prefix.rstrip('/')}/{reason}/{file_name}"
        self.move_object(source_object, destination)
        logger.info(f"Moved {source_object} -> {destination} (reason: {reason})")
        return destination

    def delete_object(self, object_name: str) -> None:
        """Delete an object from bucket (used for test cleanup)."""
        if self.in_memory:
            bucket_data = self._mem_buckets.get(self.config.bucket, {})
            bucket_data.pop(object_name, None)
            return

        try:
            client = self._get_client()
            client.remove_object(self.config.bucket, object_name)
        except Exception as e:
            logger.warning(f"Could not delete object '{object_name}': {e}")
