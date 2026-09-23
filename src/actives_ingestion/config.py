"""Configuration management for Actives Ingestion Pipeline.

Pulls configuration from environment variables or Airflow Variables/Connections.
Never stores plain-text credentials in source code.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class MinioConfig:
    """MinIO Object Storage configuration."""
    endpoint: str = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", "34.130.209.7:32010"))
    access_key: str = field(default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", ""))
    secret_key: str = field(default_factory=lambda: os.getenv("MINIO_SECRET_KEY", ""))
    secure: bool = field(default_factory=lambda: os.getenv("MINIO_SECURE", "false").lower() in ("true", "1", "yes"))
    bucket: str = field(default_factory=lambda: os.getenv("MINIO_BUCKET", "crushftp"))
    landing_prefix: str = field(default_factory=lambda: os.getenv("MINIO_LANDING_PREFIX", "mediation/landing/"))
    validated_prefix: str = field(default_factory=lambda: os.getenv("MINIO_VALIDATED_PREFIX", "mediation/validated/"))
    rejected_prefix: str = field(default_factory=lambda: os.getenv("MINIO_REJECTED_PREFIX", "mediation/rejected/"))


@dataclass(frozen=True)
class DorisConfig:
    """Apache Doris configuration for MySQL query and Stream Load interfaces."""
    host: str = field(default_factory=lambda: os.getenv("DORIS_HOST", "34.130.209.7"))
    port: int = field(default_factory=lambda: int(os.getenv("DORIS_PORT", "31545")))
    http_port: int = field(default_factory=lambda: int(os.getenv("DORIS_HTTP_PORT", "32713")))
    user: str = field(default_factory=lambda: os.getenv("DORIS_USER", "root"))
    password: str = field(default_factory=lambda: os.getenv("DORIS_PASSWORD", ""))
    database: str = field(default_factory=lambda: os.getenv("DORIS_DATABASE", "actives_db"))
    raw_table: str = field(default_factory=lambda: os.getenv("DORIS_RAW_TABLE", "raw_actives_daily"))
    rejected_table: str = field(default_factory=lambda: os.getenv("DORIS_REJECTED_TABLE", "raw_actives_daily_rejected"))
    tracking_table: str = field(default_factory=lambda: os.getenv("DORIS_TRACKING_TABLE", "actives_file_tracking"))
    connect_timeout: int = field(default_factory=lambda: int(os.getenv("DORIS_CONNECT_TIMEOUT", "10")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "5000")))


@dataclass(frozen=True)
class AirflowConfig:
    """Remote Apache Airflow configuration (Airflow 3 UI & REST API)."""
    host: str = field(default_factory=lambda: os.getenv("AIRFLOW_HOST", "34.130.209.7"))
    port: int = field(default_factory=lambda: int(os.getenv("AIRFLOW_PORT", "32002")))
    user: str = field(default_factory=lambda: os.getenv("AIRFLOW_USER", ""))
    password: str = field(default_factory=lambda: os.getenv("AIRFLOW_PASSWORD", ""))
    version: str = field(default_factory=lambda: os.getenv("AIRFLOW_VERSION", "3.3.2"))
    ui_url: str = field(default_factory=lambda: os.getenv("AIRFLOW_UI_URL", "http://34.130.209.7:32002"))
    api_url: str = field(default_factory=lambda: os.getenv("AIRFLOW_API_URL", "http://34.130.209.7:32002/api/v2"))


@dataclass(frozen=True)
class PipelineConfig:
    """Global pipeline settings."""
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "5000")))
    minio: MinioConfig = field(default_factory=MinioConfig)
    doris: DorisConfig = field(default_factory=DorisConfig)
    airflow: AirflowConfig = field(default_factory=AirflowConfig)


pipeline_config = PipelineConfig()
