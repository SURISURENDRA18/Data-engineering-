from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
  MINIO_CONN_ID: str = "santhosh_minio"
  DORIS_CONN_ID: str = "santhosh_doris"

  BUCKET_NAME: str = "crushftp"

  # Read from the shared landing folder
  LANDING_PREFIX: str = "mediation/landing/"

  # Save your results into your own subfolder so no one else is affected
  VALIDATED_PREFIX: str = "mediation/santhosh/validated/"
  REJECTED_PREFIX: str = "mediation/santhosh/rejected/"