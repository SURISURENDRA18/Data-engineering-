import gzip
import io
import re
from typing import Dict, List, Optional, Set, Tuple


class FileValidator:
  # Expected 46 headers in order
  EXPECTED_HEADERS: List[str] = [
      "CompanyID",
      "CompanyName",
      "Invoice Number",
      "BusinessUnitName",
      "DepartmentName",
      "CostCenterName",
      "Commercial OfferName",
      "BusinessUnitLevel",
      "Business Unit TAG",
      "Shared Balance Used",
      "AccountNumber",
      "CustomerNumber",
      "MSISDN",
      "ProductStatus",
      "ProvProductType",
      "Activation Date",
      "LastProductStatusChangeDate",
      "Commercial OfferDescription",
      "Last TrafficDate",
      "ProductID",
      "ICCID",
      "ExtraMSISDN_1",
      "ExtraCreation Date_1",
      "ExtraMSISDN_2",
      "ExtraCreationDate_2",
      "ExtraMSISDN_3",
      "ExtraCreationDate_3",
      "ExtraMSISDN 4",
      "ExtraCreationDate_4",
      "PlanName",
      "PlanStartDate",
      "Carrier_Name",
      "CDP Platform",
      "Carrier_AccountNo",
      "ProviderName",
      "Type",
      "Profile_version",
      "Rate_Plan_Name",
      "Rate_Plan_Code_ID",
      "Zone_Type",
      "Whitelisted Zones",
      "Blacklisted_Zones",
      "SIM_Batch",
      "SIM_Vendor",
      "Sim_Type",
      "EID",
  ]

  FILENAME_REGEX = re.compile(
      r"^([A-Za-z]+)_([0-9]+)_(Active|Actives)_([0-9]+)_([0-9]{14})\.csv(\.gz)?$"
  )
  DELIMITER = ";"

  @classmethod
  def parse_filename(
      cls, filename: str
  ) -> Optional[Tuple[str, str, str, int, str]]:
    """Extracts (region, instance, feed_type, sequence_no, timestamp) if valid."""
    match = cls.FILENAME_REGEX.match(filename)
    if not match:
      return None
    region, instance, feed_type, seq_str, ts = match.groups()[:5]
    return region, instance, feed_type, int(seq_str), ts

  @staticmethod
  def _get_stream(raw_bytes: bytes, filename: str) -> io.TextIOWrapper:
    byte_stream = io.BytesIO(raw_bytes)
    if filename.endswith(".gz"):
      decompressed = gzip.GzipFile(fileobj=byte_stream)
      return io.TextIOWrapper(decompressed, encoding="utf-8")
    return io.TextIOWrapper(byte_stream, encoding="utf-8")

  @classmethod
  def validate_file(
      cls,
      filename: str,
      raw_bytes: bytes,
      processed_filenames: Set[str],
      region_sequences: Dict[str, List[int]],
  ) -> Tuple[bool, Optional[str]]:
    """Runs all 8 checks in order. Stops at the first failure."""

    # 1. pattern_mismatch
    parsed = cls.parse_filename(filename)
    if not parsed:
      return False, "pattern_mismatch"

    region, _, _, seq_num, _ = parsed

    # 2. zero size
    if len(raw_bytes) == 0:
      return False, "zero size"

    # 4. not_text_format (validate stream decodability)
    try:
      stream = cls._get_stream(raw_bytes, filename)
      header_line = stream.readline()
      if not header_line:
        return False, "header_only"
    except Exception:
      return False, "not_text_format"

    # 3. header_only
    first_data_line = stream.readline()
    if not first_data_line or not first_data_line.strip():
      return False, "header_only"

    # 5. invalid_column_count (Header verification)
    headers = [
        col.strip() for col in header_line.rstrip("\r\n").split(cls.DELIMITER)
    ]
    if len(headers) < 9 or headers != cls.EXPECTED_HEADERS:
      return False, "invalid_column_count"

    # 6. invalid_column_count (Row field count)
    expected_field_count = len(headers)
    for line in [first_data_line] + stream.readlines():
      stripped = line.rstrip("\r\n")
      if not stripped:
        continue
      if len(stripped.split(cls.DELIMITER)) != expected_field_count:
        return False, "invalid_column_count"

    # 7. duplicate
    if filename in processed_filenames:
      return False, "duplicate"

    # 8. sequence_invalid (not repeated or out of order per region)
    seen_seqs = region_sequences.get(region, [])
    if seen_seqs:
      if seq_num in seen_seqs or seq_num <= max(seen_seqs):
        return False, "sequence_invalid"

    return True, None