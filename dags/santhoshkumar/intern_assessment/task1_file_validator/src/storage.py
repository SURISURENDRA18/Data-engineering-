import logging
from typing import List, Optional, Set
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


class StorageManager:

    def __init__(self, conn_id: str, bucket_name: str) -> None:
        self.hook = S3Hook(aws_conn_id=conn_id)
        self.bucket = bucket_name

    def list_files(self, prefix: str) -> List[str]:
        """Returns object keys under the prefix, excluding directory placeholders."""
        keys = self.hook.list_keys(bucket_name=self.bucket, prefix=prefix) or []
        return [k for k in keys if not k.endswith("/")]

    def get_already_processed_filenames(
        self, validated_prefix: str, rejected_prefix: str
    ) -> Set[str]:
        """Finds all filenames already copied to validated or rejected folders."""
        validated = self.list_files(validated_prefix)
        rejected = self.list_files(rejected_prefix)
        return {key.split("/")[-1] for key in (validated + rejected)}

    def read_file_bytes(self, key: str) -> Optional[bytes]:
        """Safely reads object content into memory as raw bytes."""
        try:
            obj = self.hook.get_key(key=key, bucket_name=self.bucket)
            if not obj:
                logger.error("Object not found: %s", key)
                return None
            return obj.get()["Body"].read()
        except Exception as e:
            logger.error("Failed to read bytes for key %s: %s", key, str(e))
            return None

    def copy_file(self, source_key: str, dest_key: str) -> None:
        """Copies object to destination without deleting the original source."""
        self.hook.copy_object(
            source_bucket_key=source_key,
            dest_bucket_key=dest_key,
            source_bucket_name=self.bucket,
            dest_bucket_name=self.bucket,
        )
        logger.info("Copied: %s -> %s", source_key, dest_key)

    def move_to_validated(self, source_key: str, validated_prefix: str, filename: str) -> None:
        """Copies file to validated destination prefix."""
        dest_key = f"{validated_prefix.rstrip('/')}/{filename}"
        self.copy_file(source_key, dest_key)

    def move_to_rejected(
        self, source_key: str, rejected_prefix: str, reason: str, filename: str
    ) -> None:
        """Copies file to rejected/<reason>/ destination prefix."""
        dest_key = f"{rejected_prefix.rstrip('/')}/{reason}/{filename}"
        self.copy_file(source_key, dest_key)