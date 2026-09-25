from __future__ import annotations

import os
import tempfile
from pathlib import Path

from .config import (
    BUCKET_NAME,
    CONTROL_TABLE,
    LANDING_PREFIX,
    REJECTED_PREFIX,
    VALIDATED_PREFIX,
)
from .doris_client import execute, execute_many
from .minio_client import (
    download_object,
    get_minio_client,
    list_files,
    move_object,
)
from .validator import validate_file


def ensure_control_table():
    """
    Store information about files already processed.

    This gives us:
    - exact duplicate detection
    - sequence tracking
    - idempotency
    """

    query = f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
        file_name VARCHAR(255),
        region VARCHAR(64),
        sequence_number BIGINT,
        checksum VARCHAR(64),
        status VARCHAR(32),
        processed_at DATETIME
    )
    DUPLICATE KEY(file_name)
    DISTRIBUTED BY HASH(file_name) BUCKETS 4
    PROPERTIES ("replication_num" = "1");
    """

    execute(query)


def get_processed_checksums() -> set[str]:
    """
    Get checksums of files that have already been processed.
    """

    rows = execute(
        f"""
        SELECT checksum
        FROM {CONTROL_TABLE}
        WHERE checksum IS NOT NULL
        """
    )

    return {row[0] for row in rows}


def get_max_sequence(region: str):
    """
    Get highest processed sequence for a region.
    """

    rows = execute(
        f"""
        SELECT MAX(sequence_number)
        FROM {CONTROL_TABLE}
        WHERE region = %s
          AND status = 'VALIDATED'
        """,
        (region,),
    )

    if not rows or rows[0][0] is None:
        return None

    return int(rows[0][0])


def register_file(
    file_name: str,
    region: str,
    sequence: int,
    checksum: str,
    status: str,
):
    execute(
        f"""
        INSERT INTO {CONTROL_TABLE}
        (
            file_name,
            region,
            sequence_number,
            checksum,
            status,
            processed_at
        )
        VALUES (%s, %s, %s, %s, %s, NOW())
        """,
        (
            file_name,
            region,
            sequence,
            checksum,
            status,
        ),
    )


def process_files():
    """
    Task 1:
        Scan landing
        Validate files
        Move valid files to validated/
        Move invalid files to rejected/<reason>/
    """

    ensure_control_table()

    minio = get_minio_client()

    objects = list_files(
        minio,
        BUCKET_NAME,
        LANDING_PREFIX,
    )

    existing_checksums = get_processed_checksums()

    processed = 0
    validated = 0
    rejected = 0

    for obj in objects:

        object_name = obj.object_name

        if object_name.endswith("/"):
            continue

        file_name = os.path.basename(object_name)

        processed += 1

        print(f"Processing file: {file_name}")

        with tempfile.TemporaryDirectory() as temp_dir:

            local_path = str(
                Path(temp_dir) / file_name
            )

            download_object(
                minio,
                BUCKET_NAME,
                object_name,
                local_path,
            )

            # Extract region only if filename is valid.
            parsed_preview = None

            try:
                from .validator import parse_filename

                parsed_preview = parse_filename(file_name)
            except Exception:
                parsed_preview = None

            region = (
                parsed_preview["region"]
                if parsed_preview
                else "UNKNOWN"
            )

            max_sequence = (
                get_max_sequence(region)
                if parsed_preview
                else None
            )

            is_valid, reason, metadata = validate_file(
                local_path,
                file_name,
                existing_checksums,
                max_sequence,
            )

            if is_valid:

                destination = (
                    f"{VALIDATED_PREFIX}{file_name}"
                )

                move_object(
                    minio,
                    BUCKET_NAME,
                    object_name,
                    destination,
                )

                register_file(
                    file_name=file_name,
                    region=metadata["region"],
                    sequence=metadata["sequence"],
                    checksum=metadata["checksum"],
                    status="VALIDATED",
                )

                existing_checksums.add(
                    metadata["checksum"]
                )

                validated += 1

                print(
                    f"VALIDATED: {file_name}"
                )

            else:

                destination = (
                    f"{REJECTED_PREFIX}"
                    f"{reason}/"
                    f"{file_name}"
                )

                move_object(
                    minio,
                    BUCKET_NAME,
                    object_name,
                    destination,
                )

                # Register duplicate/invalid files too.
                if metadata and "checksum" in metadata:
                    checksum = metadata["checksum"]
                else:
                    from .validator import calculate_md5

                    checksum = calculate_md5(local_path)

                register_file(
                    file_name=file_name,
                    region=(
                        metadata["region"]
                        if metadata
                        else "UNKNOWN"
                    ),
                    sequence=(
                        metadata["sequence"]
                        if metadata
                        else 0
                    ),
                    checksum=checksum,
                    status=f"REJECTED:{reason}",
                )

                rejected += 1

                print(
                    f"REJECTED: {file_name} "
                    f"reason={reason}"
                )

    print(
        f"Task 1 complete. "
        f"Processed={processed}, "
        f"Validated={validated}, "
        f"Rejected={rejected}"
    )