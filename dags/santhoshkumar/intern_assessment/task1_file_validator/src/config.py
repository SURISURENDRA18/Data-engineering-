from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    MINIO_CONN_ID: str = "santhosh_minio"
    DORIS_CONN_ID: str = "santhosh_doris"

    BUCKET_NAME: str = "crushftp"

    # Read from the shared landing folder
    LANDING_PREFIX: str = "mediation/landing/"

    # Save results into your own subfolder
    VALIDATED_PREFIX: str = "mediation/santhosh/validated/"
    REJECTED_PREFIX: str = "mediation/santhosh/rejected/"
    AUDIT_PREFIX: str = "mediation/santhosh/audit/"

DELIMITER = ";"

FILENAME_PATTERN = r"^([A-Za-z]+)_([0-9]+)_(Active|Actives)_([0-9]+)_([0-9]+)\.csv(\.gz)?$"

EXPECTED_HEADERS = [
    "CompanyID", "CompanyName", "Invoice Number", "BusinessUnitName", "DepartmentName",
    "CostCenterName", "Commercial OfferName", "BusinessUnitLevel", "Business Unit TAG",
    "Shared Balance Used", "AccountNumber", "CustomerNumber", "MSISDN", "ProductStatus",
    "ProvProductType", "Activation Date", "LastProductStatusChangeDate", "Commercial OfferDescription",
    "Last TrafficDate", "ProductID", "ICCID", "ExtraMSISDN_1", "ExtraCreation Date_1",
    "ExtraMSISDN_2", "ExtraCreationDate_2", "ExtraMSISDN_3", "ExtraCreationDate_3",
    "ExtraMSISDN 4", "ExtraCreationDate_4", "PlanName", "PlanStartDate", "Carrier_Name",
    "CDP Platform", "Carrier_AccountNo", "ProviderName", "Type", "Profile_version",
    "Rate_Plan_Name", "Rate_Plan_Code_ID", "Zone_Type", "Whitelisted Zones",
    "Blacklisted_Zones", "SIM_Batch", "SIM_Vendor", "Sim_Type", "EID"
]