"""Unit tests for RowDeduplicator."""

from actives_ingestion.validators import RowDeduplicator


def test_deduplication_duplicate_pair():
    rows = [
        {"CustomerNumber": "CUST1", "ICCID": "ICCID_A", "PlanName": "OldPlan"},
        {"CustomerNumber": "CUST1", "ICCID": "ICCID_A", "PlanName": "NewPlan"},
    ]
    deduped = RowDeduplicator.deduplicate(rows)
    assert len(deduped) == 1
    # Latest occurrence in file must be kept
    assert deduped[0]["PlanName"] == "NewPlan"


def test_deduplication_independent_pairs_retained():
    rows = [
        {"CustomerNumber": "CUST1", "ICCID": "ICCID_A", "PlanName": "PlanA"},
        {"CustomerNumber": "CUST2", "ICCID": "ICCID_B", "PlanName": "PlanB"},
        {"CustomerNumber": "CUST3", "ICCID": "ICCID_C", "PlanName": "PlanC"},
    ]
    deduped = RowDeduplicator.deduplicate(rows)
    assert len(deduped) == 3


def test_deduplication_multiple_duplicates_and_unique():
    rows = [
        {"CustomerNumber": "CUST1", "ICCID": "ICCID_1", "version": "v1"},
        {"CustomerNumber": "CUST2", "ICCID": "ICCID_2", "version": "v1"},
        {"CustomerNumber": "CUST1", "ICCID": "ICCID_1", "version": "v2"},
        {"CustomerNumber": "CUST1", "ICCID": "ICCID_1", "version": "v3"},
        {"CustomerNumber": "CUST3", "ICCID": "ICCID_3", "version": "v1"},
    ]
    deduped = RowDeduplicator.deduplicate(rows)
    assert len(deduped) == 3

    cust1_record = next(r for r in deduped if r["CustomerNumber"] == "CUST1")
    assert cust1_record["version"] == "v3"


def test_same_iccid_with_different_customer_numbers_is_not_collapsed():
    rows = [
        {"CustomerNumber": "CUST1", "ICCID": "ICCID_SHARED", "PlanName": "PlanA"},
        {"CustomerNumber": "CUST2", "ICCID": "ICCID_SHARED", "PlanName": "PlanB"},
    ]
    deduped = RowDeduplicator.deduplicate(rows)
    assert len(deduped) == 2
