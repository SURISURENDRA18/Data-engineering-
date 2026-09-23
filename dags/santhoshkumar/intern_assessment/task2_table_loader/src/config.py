from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class LoaderConfig:
    """Centralized configuration settings for the Task 2 Doris table loader pipeline."""

    # Airflow connection identifier for Apache Doris database
    doris_conn_id: str = "santhosh_doris"

    # Airflow connection identifier for MinIO / S3 storage
    minio_conn_id: str = "santhosh_minio"

    # Target MinIO bucket name containing the validated data
    bucket_name: str = "crushftp"

    # MinIO key prefix path where Task 1 deposited validated files
    source_prefix: str = "mediation/santhosh/validated/"

    # Target Apache Doris database name
    database_name: str = "santhosh_actives_db"

    # Destination table for valid, deduplicated records
    target_table: str = "raw_actives"

    # Destination table for rows violating data quality checks
    reject_table: str = "raw_actives_rejected"

    # Destination audit table tracking processed files for idempotency
    audit_table: str = "loaded_files_audit"

    # Exact 46-column canonical order expected in the Actives feed
    expected_columns: List[str] = field(default_factory=lambda: [
        "companyid", "companyname", "invoicenumber", "businessunitname", "departmentname",
        "costcentername", "commercialoffername", "businessunitlevel", "businessunittag",
        "sharedbalanceused", "accountnumber", "customernumber", "msisdn", "productstatus",
        "provproducttype", "activationdate", "lastproductstatuschangedate",
        "commercialofferdescription", "lasttrafficdate", "productid", "iccid",
        "extramsisdn1", "extracreationdate1", "extramsisdn2", "extracreationdate2",
        "extramsisdn3", "extracreationdate3", "extramsisdn4", "extracreationdate4",
        "planname", "planstartdate", "carriername", "cdpplatform", "carrieraccountno",
        "providername", "type", "profileversion", "rateplanname", "rateplancodeid",
        "zonetype", "whitelistedzones", "blacklistedzones", "simbatch", "simvendor",
        "simtype", "eid"
    ])