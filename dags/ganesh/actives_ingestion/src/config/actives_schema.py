"""Canonical schema/constants for the Actives feed. Nothing else in this
project should redefine the column list, filename pattern, or reason
strings — everything imports from here."""

import re

CSV_DELIMITER = ";"

FILENAME_PATTERN = re.compile(
    r"^[A-Za-z]+_[0-9]+_(Active|Actives)_[0-9]+_[0-9]+\.csv(\.gz)?$"
)

# The 46 expected CSV header columns, in exact required order.
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
assert EXPECTED_COLUMN_COUNT == 46

# Maps each source CSV header to its column name in raw_actives_daily.
# Pinned once, here — no other module should re-derive this mapping.
COLUMN_DB_MAP = {
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
assert set(COLUMN_DB_MAP) == set(EXPECTED_COLUMNS)

# Doris raw table columns, in DDL order (excluding file_name/loaded_at
# metadata columns, which are added separately by the loader).
RAW_TABLE_DATA_COLUMNS = [COLUMN_DB_MAP[c] for c in EXPECTED_COLUMNS]


class RejectionReason:
    """File-level rejection reasons (Task 1), in check-order."""

    PATTERN_MISMATCH = "pattern_mismatch"
    ZERO_SIZE = "zero_size"
    HEADER_ONLY = "header_only"
    NOT_TEXT_FORMAT = "not_text_format"
    INVALID_COLUMN_COUNT = "invalid_column_count"
    DUPLICATE = "duplicate"
    SEQUENCE_INVALID = "sequence_invalid"


class RowRejectReason:
    """Row-level rejection reasons (Task 2)."""

    NULL_OR_EMPTY_ICCID = "NULL_OR_EMPTY_ICCID"
    NULL_OR_EMPTY_CUSTOMER_NUMBER = "NULL_OR_EMPTY_CUSTOMER_NUMBER"
    NULL_OR_ZERO_COMPANY_ID = "NULL_OR_ZERO_COMPANY_ID"


class ValidationStatus:
    VALIDATED = "VALIDATED"
    REJECTED = "REJECTED"


class LoadStatus:
    PENDING = "PENDING"
    LOADED = "LOADED"
    LOAD_FAILED = "LOAD_FAILED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
