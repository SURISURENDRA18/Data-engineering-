import gzip
import io
import json
import logging
import re
from typing import Dict, List, Optional, Set, Tuple

from src.config import DELIMITER, EXPECTED_HEADERS, FILENAME_PATTERN

logger = logging.getLogger(__name__)


class FileValidator:
    DELIMITER = DELIMITER
    EXPECTED_HEADERS = EXPECTED_HEADERS
    FILENAME_REGEX = re.compile(FILENAME_PATTERN)

    @classmethod
    def parse_filename(cls, filename: str) -> Optional[Tuple[str, str, str, int, str]]:
        match = cls.FILENAME_REGEX.match(filename)
        if not match:
            return None
        region, instance, feed_type, seq_str, ts = match.groups()[:5]
        return region, instance, feed_type, int(seq_str), ts

    @staticmethod
    def _get_text_stream(raw_bytes: bytes, filename: str) -> Optional[io.StringIO]:
        try:
            if filename.endswith(".gz"):
                decompressed = gzip.decompress(raw_bytes)
                text = decompressed.decode("utf-8-sig")
            else:
                text = raw_bytes.decode("utf-8-sig")

            if "\x00" in text:
                return None
            return io.StringIO(text)
        except Exception:
            return None

    @classmethod
    def validate_file(
        cls,
        filename: str,
        raw_bytes: bytes,
        processed_filenames: Set[str],
        region_sequences: Dict[str, List[int]],
    ) -> Tuple[bool, Optional[str]]:
        # 1. pattern_mismatch
        parsed = cls.parse_filename(filename)
        if not parsed:
            return False, "pattern_mismatch"

        region, instance, feed_type, seq_num, ts = parsed

        # 2. zero_size
        if len(raw_bytes) == 0:
            return False, "zero_size"

        stream = cls._get_text_stream(raw_bytes, filename)

        # 3. header_only
        if stream is not None:
            first_line = stream.readline()
            second_line = stream.readline()
            if not first_line.strip() or not second_line.strip():
                return False, "header_only"
            stream.seek(0)
        else:
            # 4. not_text_format
            return False, "not_text_format"

        # 5. invalid_column_count (Header verification)
        header_line = stream.readline()
        headers = [col.strip().strip('"') for col in header_line.rstrip("\r\n").split(cls.DELIMITER)]
        if len(headers) < 9 or headers != cls.EXPECTED_HEADERS:
            return False, "invalid_column_count"

        # 6. Row verification: allow rows that have at least mandatory columns (>= 21) up to 46
        expected_field_count = len(cls.EXPECTED_HEADERS)
        for row_idx, line in enumerate(stream, start=2):
            stripped = line.rstrip("\r\n")
            if not stripped:
                continue
            field_count = len(stripped.split(cls.DELIMITER))
            if field_count < 21 or field_count > expected_field_count:
                return False, "invalid_column_count"

        # 7. duplicate
        if filename in processed_filenames:
            return False, "duplicate"

        # 8. sequence_invalid
        seen_seqs = region_sequences.get(region, [])
        if seen_seqs:
            if seq_num in seen_seqs or seq_num <= max(seen_seqs):
                return False, "sequence_invalid"

        return True, None

    @classmethod
    def extract_valid_rows_and_audit(
        cls, filename: str, raw_bytes: bytes
    ) -> Tuple[Optional[bytes], Optional[str], int, int]:
        """
        Pads rows with missing trailing columns to 46 columns.
        """
        stream = cls._get_text_stream(raw_bytes, filename)
        if not stream:
            return None, None, 0, 0

        header_line = stream.readline()
        cleaned_lines = [header_line.rstrip("\r\n")]
        expected_field_count = len(cls.EXPECTED_HEADERS)

        dropped_rows = []
        valid_count = 0

        for row_idx, line in enumerate(stream, start=2):
            stripped = line.rstrip("\r\n")
            if not stripped:
                continue
            fields = stripped.split(cls.DELIMITER)
            
            # Row has enough fields to cover mandatory keys and does not exceed 46
            if 21 <= len(fields) <= expected_field_count:
                if len(fields) < expected_field_count:
                    fields = fields + [""] * (expected_field_count - len(fields))
                cleaned_lines.append(cls.DELIMITER.join(fields))
                valid_count += 1
            else:
                dropped_rows.append({
                    "row_number": row_idx,
                    "field_count": len(fields),
                    "expected": expected_field_count,
                    "reason": "ROW_FIELD_COUNT_OUT_OF_BOUNDS"
                })

        audit_manifest = {
            "file_name": filename,
            "total_scanned_rows": valid_count + len(dropped_rows),
            "valid_rows_retained": valid_count,
            "dropped_rows_count": len(dropped_rows),
            "dropped_details_sample": dropped_rows[:50]
        }

        if valid_count > 0:
            cleaned_text = "\n".join(cleaned_lines) + "\n"
            if filename.endswith(".gz"):
                out_buf = io.BytesIO()
                with gzip.GzipFile(fileobj=out_buf, mode="wb") as gz:
                    gz.write(cleaned_text.encode("utf-8"))
                cleaned_bytes = out_buf.getvalue()
            else:
                cleaned_bytes = cleaned_text.encode("utf-8")
        else:
            cleaned_bytes = None

        return cleaned_bytes, json.dumps(audit_manifest, indent=2), valid_count, len(dropped_rows)