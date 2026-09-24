import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    # Airflow Connections
    MINIO_CONN_ID: str = "santhosh_minio"
    DORIS_CONN_ID: str = "santhosh_doris"

    # Bucket
    BUCKET_NAME: str = "crushftp"

    # Prefixes
    LANDING_PREFIX: str = "mediation/landing/"
    VALIDATED_PREFIX: str = "mediation/santhosh/validated/"
    REJECTED_PREFIX: str = "mediation/santhosh/rejected/"
    AUDIT_PREFIX: str = "mediation/santhosh/audit/"

    DELIMITER: str = ";"

    FILENAME_PATTERN: str = r"^([A-Za-z]+)_([0-9]+)_(Active|Actives)_([0-9]+)_([0-9]+)\.csv(\.gz)?$"


MINIO_CONN_ID = PipelineConfig.MINIO_CONN_ID
DORIS_CONN_ID = PipelineConfig.DORIS_CONN_ID
BUCKET_NAME = PipelineConfig.BUCKET_NAME
LANDING_PREFIX = PipelineConfig.LANDING_PREFIX
VALIDATED_PREFIX = PipelineConfig.VALIDATED_PREFIX
REJECTED_PREFIX = PipelineConfig.REJECTED_PREFIX
AUDIT_PREFIX = PipelineConfig.AUDIT_PREFIX
DELIMITER = PipelineConfig.DELIMITER
FILENAME_PATTERN = PipelineConfig.FILENAME_PATTERN

# 46 Canonical Headers matching feed specifications exactly (no spaces)
EXPECTED_HEADERS = [
    "CompanyID",
    "CompanyName",
    "InvoiceNumber",
    "BusinessUnitName",
    "DepartmentName",
    "CostCenterName",
    "CommercialOfferName",
    "BusinessUnitLevel",
    "BusinessUnitTAG",
    "SharedBalanceUsed",
    "AccountNumber",
    "CustomerNumber",
    "MSISDN",
    "ProductStatus",
    "ProvProductType",
    "ActivationDate",
    "LastProductStatusChangeDate",
    "CommercialOfferDescription",
    "LastTrafficDate",
    "ProductID",
    "ICCID",
    "ExtraMSISDN_1",
    "ExtraCreationDate_1",
    "ExtraMSISDN_2",
    "ExtraCreationDate_2",
    "ExtraMSISDN_3",
    "ExtraCreationDate_3",
    "ExtraMSISDN_4",
    "ExtraCreationDate_4",
    "PlanName",
    "PlanStartDate",
    "Carrier_Name",
    "CDP_Platform",
    "Carrier_AccountNo",
    "ProviderName",
    "Type",
    "Profile_version",
    "Rate_Plan_Name",
    "Rate_Plan_Code_ID",
    "Zone_Type",
    "Whitelisted_Zones",
    "Blacklisted_Zones",
    "SIM_Batch",
    "SIM_Vendor",
    "Sim_Type",
    "EID",
]