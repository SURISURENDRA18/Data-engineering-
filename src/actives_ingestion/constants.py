"""Constants for Actives Ingestion Pipeline."""

import re

# Filename regex pattern matching Check 1 and Check 8
FILENAME_REGEX = re.compile(
    r"^(?P<region>[A-Za-z]+)_(?P<instance>[0-9]+)_(?P<feed>Active|Actives)_(?P<sequence>[0-9]+)_(?P<timestamp>[0-9]+)\.csv(?P<compression>\.gz)?$"
)

# Expected 46 CSV columns in exact order
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

EXPECTED_COLUMN_COUNT = len(EXPECTED_COLUMNS)

# Map CSV headers to Doris raw_actives_daily columns
CSV_TO_DORIS_COLUMN_MAPPING = {
    "CompanyID": "companyid",
    "CompanyName": "companyname",
    "InvoiceNumber": "invoicenumber",
    "BusinessUnitName": "businessunitname",
    "DepartmentName": "departmentname",
    "CostCenterName": "costcentername",
    "CommercialOfferName": "commercialoffername",
    "BusinessUnitLevel": "businessunitlevel",
    "BusinessUnitTAG": "businessunittag",
    "SharedBalanceUsed": "sharedbalanceused",
    "AccountNumber": "accountnumber",
    "CustomerNumber": "customernumber",
    "MSISDN": "msisdn",
    "ProductStatus": "productstatus",
    "ProvProductType": "provproducttype",
    "ActivationDate": "activationdate",
    "LastProductStatusChangeDate": "lastproductstatuschangedate",
    "CommercialOfferDescription": "commercialofferdescription",
    "LastTrafficDate": "lasttrafficdate",
    "ProductID": "productid",
    "ICCID": "iccid",
    "ExtraMSISDN_1": "extramsisdn1",
    "ExtraCreationDate_1": "extracreationdate1",
    "ExtraMSISDN_2": "extramsisdn2",
    "ExtraCreationDate_2": "extracreationdate2",
    "ExtraMSISDN_3": "extramsisdn3",
    "ExtraCreationDate_3": "extracreationdate3",
    "ExtraMSISDN_4": "extramsisdn4",
    "ExtraCreationDate_4": "extracreationdate4",
    "PlanName": "planname",
    "PlanStartDate": "planstartdate",
    "Carrier_Name": "carriername",
    "CDP_Platform": "cdpplatform",
    "Carrier_AccountNo": "carrieraccountno",
    "ProviderName": "providername",
    "Type": "type",
    "Profile_version": "profileversion",
    "Rate_Plan_Name": "rateplanname",
    "Rate_Plan_Code_ID": "rateplancodeid",
    "Zone_Type": "zonetype",
    "Whitelisted_Zones": "whitelistedzones",
    "Blacklisted_Zones": "blacklistedzones",
    "SIM_Batch": "simbatch",
    "SIM_Vendor": "simvendor",
    "Sim_Type": "simtype",
    "EID": "eid",
}

# File rejection reason codes (exact strings required by Task 1)
REASON_PATTERN_MISMATCH = "pattern_mismatch"
REASON_ZERO_SIZE = "zero_size"
REASON_HEADER_ONLY = "header_only"
REASON_NOT_TEXT_FORMAT = "not_text_format"
REASON_INVALID_COLUMN_COUNT = "invalid_column_count"
REASON_DUPLICATE = "duplicate"
REASON_SEQUENCE_INVALID = "sequence_invalid"

# Row rejection reasons (exact strings required by Task 2)
ROW_REASON_NULL_OR_EMPTY_ICCID = "NULL_OR_EMPTY_ICCID"
ROW_REASON_NULL_OR_EMPTY_CUSTOMER_NUMBER = "NULL_OR_EMPTY_CUSTOMER_NUMBER"
ROW_REASON_NULL_OR_ZERO_COMPANY_ID = "NULL_OR_ZERO_COMPANY_ID"

# File processing statuses
STATUS_PROCESSING = "PROCESSING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
