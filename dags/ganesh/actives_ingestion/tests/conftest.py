"""Shared test fixtures: a hand-rolled fake `minio` client (the real minio
library has no equivalent of moto, so this mimics just the surface our
code calls), and a hand-rolled fake Doris connection that specifically
emulates Doris's UNIQUE-KEY-replace-on-insert semantics (deliberately not
sqlite, whose constraint/upsert behavior differs from Doris's model).
"""

import pytest
from minio.error import S3Error

from src.config.actives_schema import RAW_TABLE_DATA_COLUMNS
from src.config.settings import Settings

TEST_BUCKET = "crushftp"


class FakeMinioObject:
    def __init__(self, object_name: str, size: int, is_dir: bool = False):
        self.object_name = object_name
        self.size = size
        self.is_dir = is_dir


class FakeMinioResponse:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data

    def close(self):
        pass

    def release_conn(self):
        pass


def _not_found(object_name: str) -> S3Error:
    return S3Error(
        response=None,
        code="NoSuchKey",
        message="Object does not exist",
        resource=object_name,
        request_id=None,
        host_id=None,
    )


class FakeMinioClient:
    """Mimics the subset of `minio.Minio`'s API that
    src/clients/minio_client.py calls, backed by an in-memory dict."""

    def __init__(self):
        self._buckets: dict = {}

    def create_bucket(self, bucket_name: str) -> None:
        self._buckets.setdefault(bucket_name, {})

    def put_object(self, bucket_name: str = None, object_name: str = None, data=None, length: int = -1, **kwargs) -> None:
        """Accepts either the real minio positional signature or, purely
        for terse test setup, boto3-style Bucket=/Key=/Body= kwargs."""
        bucket_name = bucket_name or kwargs.get("Bucket")
        object_name = object_name or kwargs.get("Key")
        data = data if data is not None else kwargs.get("Body")
        content = data.read() if hasattr(data, "read") else data
        self._buckets.setdefault(bucket_name, {})[object_name] = content

    def list_objects(self, bucket_name: str, prefix: str = None, recursive: bool = False):
        store = self._buckets.get(bucket_name, {})
        prefix = prefix or ""
        for key, content in list(store.items()):
            if key.startswith(prefix):
                yield FakeMinioObject(key, len(content))

    def get_object(self, bucket_name: str, object_name: str) -> FakeMinioResponse:
        store = self._buckets.get(bucket_name, {})
        if object_name not in store:
            raise _not_found(object_name)
        return FakeMinioResponse(store[object_name])

    def stat_object(self, bucket_name: str, object_name: str):
        store = self._buckets.get(bucket_name, {})
        if object_name not in store:
            raise _not_found(object_name)
        return True

    def copy_object(self, bucket_name: str, object_name: str, source) -> None:
        content = self._buckets[source.bucket_name][source.object_name]
        self._buckets.setdefault(bucket_name, {})[object_name] = content


def _strip_sql_comments(sql: str) -> str:
    """Our .sql files lead with '-- PURPOSE:' documentation comments, so the
    real statement keyword isn't the first token in the raw string."""
    lines = [line for line in sql.strip().splitlines() if not line.strip().startswith("--")]
    return "\n".join(lines).strip()

TRACKING_COLUMNS = [
    "file_checksum",
    "file_name",
    "region",
    "sequence_number",
    "landing_key",
    "current_key",
    "validation_status",
    "rejection_reason",
    "validation_detail",
    "load_status",
    "row_count_total",
    "row_count_loaded",
    "row_count_rejected",
    "row_count_duplicate_dropped",
    "file_size_bytes",
    "validated_at",
    "loaded_at",
    "updated_at",
]
TRACKING_SELECT_COLUMNS = [c for c in TRACKING_COLUMNS if c != "updated_at"]
RAW_COLUMNS = ["file_name"] + RAW_TABLE_DATA_COLUMNS + ["loaded_at"]
REJECTED_COLUMNS = [
    "file_name",
    "row_number",
    "iccid",
    "customernumber",
    "companyid",
    "reject_reason",
    "raw_row_json",
    "rejected_at",
]


class FakeCursor:
    def __init__(self, store: "FakeDorisStore"):
        self._store = store
        self._last_result = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, params=None):
        params = params or ()
        upper = _strip_sql_comments(sql).upper()
        self._last_result = []

        if upper.startswith("CREATE TABLE"):
            self.rowcount = 0
            return self.rowcount

        if "FILE_PROCESSING_LOG" in sql.upper() and upper.startswith("SELECT") and "MAX(SEQUENCE_NUMBER)" in upper:
            self._last_result = self._store.max_sequence_per_region()
            self.rowcount = len(self._last_result)
            return self.rowcount

        if "FILE_PROCESSING_LOG" in sql.upper() and upper.startswith("SELECT"):
            row = self._store.tracking.get(params[0])
            self._last_result = [self._project(row, TRACKING_SELECT_COLUMNS)] if row else []
            self.rowcount = len(self._last_result)
            return self.rowcount

        if "FILE_PROCESSING_LOG" in sql.upper() and upper.startswith("INSERT"):
            row = dict(zip(TRACKING_COLUMNS, params))
            self._store.tracking[row["file_checksum"]] = row
            self.rowcount = 1
            return self.rowcount

        if "RAW_ACTIVES_DAILY_REJECTED" in sql.upper() and upper.startswith("INSERT"):
            self._store.rejected_rows.append(dict(zip(REJECTED_COLUMNS, params)))
            self.rowcount = 1
            return self.rowcount

        if "RAW_ACTIVES_DAILY" in sql.upper() and upper.startswith("INSERT"):
            row = dict(zip(RAW_COLUMNS, params))
            self._store.raw_rows[row["iccid"]] = row  # UNIQUE KEY(iccid) replace-on-insert
            self.rowcount = 1
            return self.rowcount

        raise NotImplementedError(f"FakeCursor cannot handle SQL: {sql[:120]}")

    def executemany(self, sql: str, param_list):
        for params in param_list:
            self.execute(sql, params)
        return len(param_list)

    def fetchall(self):
        return self._last_result

    @staticmethod
    def _project(row: dict, columns) -> dict:
        return {c: row.get(c) for c in columns}


class FakeDorisStore:
    def __init__(self):
        self.tracking: dict = {}
        self.raw_rows: dict = {}
        self.rejected_rows: list = []

    def max_sequence_per_region(self):
        best: dict = {}
        for row in self.tracking.values():
            if row.get("validation_status") != "VALIDATED" or not row.get("region"):
                continue
            region = row["region"]
            seq = row["sequence_number"]
            if region not in best or seq > best[region]:
                best[region] = seq
        return [{"region": r, "max_sequence_number": s} for r, s in best.items()]


class FakeDorisConnection:
    def __init__(self):
        self.store = FakeDorisStore()

    def cursor(self):
        return FakeCursor(self.store)

    def close(self):
        pass

    def commit(self):
        pass


@pytest.fixture
def fake_doris_conn():
    return FakeDorisConnection()


@pytest.fixture
def test_settings():
    return Settings(
        bucket=TEST_BUCKET,
        landing_prefix="mediation/landing/",
        validated_prefix="mediation/validated/",
        rejected_prefix="mediation/rejected/",
        doris_database="test_db",
    )


@pytest.fixture
def s3_client():
    client = FakeMinioClient()
    client.create_bucket(TEST_BUCKET)
    return client
