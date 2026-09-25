from __future__ import annotations

from dataclasses import dataclass


# Airflow Connection IDs
MINIO_CONN_ID = "actives_minio_venky"
DORIS_CONN_ID = "actives_doris_venky"


# MinIO paths
BUCKET_NAME = "crushftp"

LANDING_PREFIX = "mediation/landing/"
VALIDATED_PREFIX = "mediation/venky/validated/"
REJECTED_PREFIX = "mediation/venky/rejected/"


# Doris
DORIS_DATABASE = "venky_actives"

RAW_TABLE = "raw_actives_daily"
REJECTED_TABLE = "raw_actives_daily"
CONTROL_TABLE = "actives_file_registry"


EXPECTED_COLUMNS = [
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


FILENAME_PATTERN = (
    r"^[A-Za-z]+_[0-9]+_(Active|Actives)_[0-9]+_[0-9]{14}\.csv(\.gz)?$"
)


@dataclass
class FileInfo:
    object_name: str
    file_name: str
    region: str
    sequence: int
    compressed: bool
    checksum: str | None = None