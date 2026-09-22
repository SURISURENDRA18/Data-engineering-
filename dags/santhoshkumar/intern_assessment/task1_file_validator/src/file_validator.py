import gzip
import io
import re
from typing import Dict, List, Optional, Set, Tuple

try:
    from src.config import DELIMITER, EXPECTED_HEADERS, FILENAME_PATTERN
except ModuleNotFoundError:
    from task1_file_validator.src.config import DELIMITER, EXPECTED_HEADERS, FILENAME_PATTERN


class FileValidator:
    DELIMITER = DELIMITER
    EXPECTED_HEADERS = EXPECTED_HEADERS
    FILENAME_REGEX = re.compile(FILENAME_PATTERN)

    @classmethod
    def parse_filename(cls, filename: str) -> Optional[Tuple[str, str, str, int, str]]:
        """Extracts (region, instance, feed_type, sequence_no, timestamp)."""
        match = cls.FILENAME_REGEX.match(filename)
        if not match:
            return None
        region, instance, feed_type, seq_str, ts = match.groups()[:5]
        return region, instance, feed_type, int(seq_str), ts

    @staticmethod
    def _get_text_stream(raw_bytes: bytes, filename: str) -> Optional[io.StringIO]:
        """Safely decompresses and decodes bytes into a text stream."""
        try:
            if filename.endswith(".gz"):
                decompressed = gzip.decompress(raw_bytes)
                text = decompressed.decode("utf-8")
            else:
                text = raw_bytes.decode("utf-8")

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
        """
        Runs all 8 checks in exact PDF order.
        Stops at the very first failure.
        """

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
        headers = [col.strip() for col in header_line.rstrip("\r\n").split(cls.DELIMITER)]
        if len(headers) < 9 or headers != cls.EXPECTED_HEADERS:
            return False, "invalid_column_count"

        # 6. invalid_column_count (Data row verification)
        expected_field_count = len(cls.EXPECTED_HEADERS)
        for line in stream:
            stripped = line.rstrip("\r\n")
            if not stripped:
                continue
            if len(stripped.split(cls.DELIMITER)) != expected_field_count:
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