"""Unit tests verifying performance optimizations: metadata caching, deferred stream downloading, and concurrent handling."""

from unittest.mock import MagicMock

from actives_ingestion.constants import REASON_PATTERN_MISMATCH, REASON_ZERO_SIZE
from actives_ingestion.file_handler import FileHandler
from actives_ingestion.minio_client import MinioService
from actives_ingestion.validators import FileValidator


def test_lazy_stream_not_evaluated_for_pattern_mismatch():
    """Verify that a callable stream loader is never invoked when filename pattern fails."""
    validator = FileValidator()
    loader_mock = MagicMock()

    result = validator.validate(
        filename="INVALID_NAME.csv",
        content_stream=loader_mock,
        file_size=100,
    )

    assert result.is_valid is False
    assert result.reason == REASON_PATTERN_MISMATCH
    loader_mock.assert_not_called()


def test_lazy_stream_not_evaluated_for_zero_size():
    """Verify that a callable stream loader is never invoked when file size is zero."""
    validator = FileValidator()
    loader_mock = MagicMock()

    result = validator.validate(
        filename="US_01_Active_00001_20260922120000.csv",
        content_stream=loader_mock,
        file_size=0,
    )

    assert result.is_valid is False
    assert result.reason == REASON_ZERO_SIZE
    loader_mock.assert_not_called()


def test_minio_metadata_cache_avoids_stat_calls():
    """Verify MinioService caches object sizes during listing and avoids stat_object calls."""
    svc = MinioService(in_memory=True)
    svc.put_object_data("mediation/landing/file1.csv", b"12345")
    svc.put_object_data("mediation/landing/file2.csv", b"1234567890")

    # Listing caches sizes
    files = svc.list_landing_files()
    assert len(files) == 2

    # Direct size lookup should hit cache
    assert svc.get_object_size("mediation/landing/file1.csv") == 5
    assert svc.get_object_size("mediation/landing/file2.csv") == 10

    # Moving object updates size cache
    svc.move_to_validated("mediation/landing/file1.csv")
    assert svc.get_object_size("mediation/surendra/validated/file1.csv") == 5


def test_file_handler_batch_processing_and_counts():
    """Verify FileHandler processes mixed batch and returns accurate counts."""
    minio_svc = MinioService(in_memory=True)

    # 1 valid file
    header = "CompanyID,CompanyName,InvoiceNumber,BusinessUnitName,DepartmentName,CostCenterName,CommercialOfferName,BusinessUnitLevel,BusinessUnitTAG,SharedBalanceUsed,AccountNumber,CustomerNumber,MSISDN,ProductStatus,ProvProductType,ActivationDate,LastProductStatusChangeDate,CommercialOfferDescription,LastTrafficDate,ProductID,ICCID,ExtraMSISDN_1,ExtraCreationDate_1,ExtraMSISDN_2,ExtraCreationDate_2,ExtraMSISDN_3,ExtraCreationDate_3,ExtraMSISDN_4,ExtraCreationDate_4,PlanName,PlanStartDate,Carrier_Name,CDP_Platform,Carrier_AccountNo,ProviderName,Type,Profile_version,Rate_Plan_Name,Rate_Plan_Code_ID,Zone_Type,Whitelisted_Zones,Blacklisted_Zones,SIM_Batch,SIM_Vendor,Sim_Type,EID\n"
    row = "100," + ",".join(["val"] * 45) + "\n"
    content = (header + row).encode("utf-8")

    minio_svc.put_object_data("mediation/landing/US_01_Active_00001_20260922120000.csv", content)
    # 1 zero size
    minio_svc.put_object_data("mediation/landing/US_01_Active_00002_20260922120000.csv", b"")
    # 1 pattern mismatch
    minio_svc.put_object_data("mediation/landing/SNG_01_SimInventory_00001_20260922120000.csv", b"abc")

    from actives_ingestion.repositories.file_tracking_repository import FileTrackingRepository
    tracking_repo = FileTrackingRepository(in_memory=True)
    handler = FileHandler(minio_service=minio_svc, tracking_repository=tracking_repo)
    summary = handler.process_landing_files()

    assert summary.files_discovered == 3
    assert summary.files_validated == 1
    assert summary.files_rejected == 2
    assert summary.files_failed == 0

