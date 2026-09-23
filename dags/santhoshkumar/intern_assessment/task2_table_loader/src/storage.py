import logging
from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Initialize module logger
logger = logging.getLogger(__name__)


class StorageManager:
    """Manages object retrieval from MinIO / S3 using Airflow connection hooks."""

    def __init__(self, conn_id: str, bucket_name: str) -> None:
        # Initialize Airflow S3Hook using the connection ID to avoid hardcoded credentials
        self.hook = S3Hook(aws_conn_id=conn_id)
        # Store the target bucket name
        self.bucket_name = bucket_name

    def list_files(self, prefix: str) -> List[str]:
        """Discovers and returns all non-directory object keys under a prefix."""
        # Query MinIO for all keys starting with the specified prefix
        keys = self.hook.list_keys(bucket_name=self.bucket_name, prefix=prefix)

        # Return empty list if no keys are found
        if not keys:
            return []

        # Filter out folder pseudo-keys (paths ending in '/')
        return [k for k in keys if not k.endswith("/")]

    def read_bytes(self, key: str) -> bytes:
        """Retrieves an object from MinIO and returns its raw binary payload."""
        # Fetch the S3 object reference by its bucket and key
        s3_obj = self.hook.get_key(key=key, bucket_name=self.bucket_name)

        # Read and return the entire byte stream from the payload body
        return s3_obj.get()["Body"].read()