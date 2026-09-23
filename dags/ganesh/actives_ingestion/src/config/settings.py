"""Environment/deployment configuration for the Actives ingestion pipeline.

Deliberately contains no secrets. MinIO/Doris credentials live in Airflow
Connections (referenced here only by connection id). Everything else
(bucket, paths, database/table names) is overridable via an Airflow
Variable named ``actives_pipeline_settings`` (a small JSON blob) so the
same code runs unmodified across environments; sensible defaults for
this assessment's confirmed environment are used otherwise.

This module must stay importable without Airflow installed, so Airflow
is only ever imported lazily, inside ``load_settings``.
"""

import json
import os
from dataclasses import dataclass, replace

AIRFLOW_VARIABLE_NAME = "actives_pipeline_settings"


@dataclass(frozen=True)
class Settings:
    minio_conn_id: str = "minio_default"
    doris_conn_id: str = "doris_default"

    bucket: str = "crushftp"
    landing_prefix: str = "mediation/landing/"  # shared source -- every candidate's pipeline reads from here
    validated_prefix: str = "mediation/ganesh/validated/"  # own output namespace, matching peers' convention
    rejected_prefix: str = "mediation/ganesh/rejected/"

    doris_database: str = "database_ganesh"
    raw_table: str = "raw_actives_daily"
    rejected_table: str = "raw_actives_daily_rejected"
    tracking_table: str = "file_processing_log"

    def rejected_prefix_for(self, reason: str) -> str:
        return f"{self.rejected_prefix}{reason}/"


def _load_overrides() -> dict:
    """Best-effort lookup of override JSON, Airflow Variable first, then
    an environment variable of the same name, for local/test runs."""
    try:
        from airflow.models import Variable

        raw = Variable.get(AIRFLOW_VARIABLE_NAME, default_var=None)
    except Exception:
        raw = os.environ.get(AIRFLOW_VARIABLE_NAME.upper())

    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}


def load_settings() -> Settings:
    overrides = _load_overrides()
    valid_fields = Settings.__dataclass_fields__.keys()
    filtered = {k: v for k, v in overrides.items() if k in valid_fields}
    return replace(Settings(), **filtered)
