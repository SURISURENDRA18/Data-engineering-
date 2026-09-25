#Task2
from __future__ import annotations

import csv
import gzip
import os
import tempfile
from datetime import datetime

from .config import (
    BUCKET_NAME,
    CONTROL_TABLE,
    RAW_TABLE,
    REJECTED_TABLE,
    VALIDATED_PREFIX,
    EXPECTED_COLUMNS,
)
from .doris_client import execute, execute_many
from .minio_client import (
    download_object,
    get_minio_client,
    list_files,
    move_object,
)


RAW_COLUMNS = [
    "file_name",
    "companyid",
    "companyname",
    "invoicenumber",
    "businessunitname",
    "departmentname",
    "costcentername",
    "commercialoffername",
    "businessunitlevel",
    "businessunittag",
    "sharedbalanceused",
    "accountnumber",
    "customernumber",
    "msisdn",
    "productstatus",
    "provproducttype",
    "activationdate",
    "lastproductstatuschangedate",
    "commercialofferdescription",
    "lasttrafficdate",
    "productid",
    "iccid",
    "extramsisdn1",
    "extracreationdate1",
    "extramsisdn2",
    "extracreationdate2",
    "extramsisdn3",
    "extracreationdate3",
    "extramsisdn4",
    "extracreationdate4",
    "planname",
    "planstartdate",
    "carriername",
    "cdpplatform",
    "carrieraccountno",
    "providername",
    "type",
    "profileversion",
    "rateplanname",
    "rateplancodeid",
    "zonetype",
    "whitelistedzones",
    "blacklistedzones",
    "simbatch",
    "simvendor",
    "simtype",
    "eid",
    "loaded_at",
]


def ensure_raw_tables():
    """
    Create raw and rejected-row tables if they do not exist.
    """

    execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            file_name VARCHAR(255),
            companyid VARCHAR(64),
            companyname VARCHAR(255),
            invoicenumber VARCHAR(64),
            businessunitname VARCHAR(255),
            departmentname VARCHAR(255),
            costcentername VARCHAR(255),
            commercialoffername VARCHAR(255),
            businessunitlevel VARCHAR(10),
            businessunittag VARCHAR(255),
            sharedbalanceused VARCHAR(50),
            accountnumber VARCHAR(64),
            customernumber VARCHAR(64),
            msisdn VARCHAR(32),
            productstatus VARCHAR(50),
            provproducttype VARCHAR(100),
            activationdate VARCHAR(30),
            lastproductstatuschangedate VARCHAR(30),
            commercialofferdescription VARCHAR(255),
            lasttrafficdate VARCHAR(30),
            productid VARCHAR(64),
            iccid VARCHAR(32) NOT NULL,
            extramsisdn1 VARCHAR(32),
            extracreationdate1 VARCHAR(30),
            extramsisdn2 VARCHAR(32),
            extracreationdate2 VARCHAR(30),
            extramsisdn3 VARCHAR(32),
            extracreationdate3 VARCHAR(30),
            extramsisdn4 VARCHAR(32),
            extracreationdate4 VARCHAR(30),
            planname VARCHAR(255),
            planstartdate VARCHAR(30),
            carriername VARCHAR(255),
            cdpplatform VARCHAR(100),
            carrieraccountno VARCHAR(64),
            providername VARCHAR(255),
            `type` VARCHAR(50),
            profileversion VARCHAR(50),
            rateplanname VARCHAR(255),
            rateplancodeid VARCHAR(64),
            zonetype VARCHAR(50),
            whitelistedzones VARCHAR(500),
            blacklistedzones VARCHAR(500),
            simbatch VARCHAR(100),
            simvendor VARCHAR(100),
            simtype VARCHAR(50),
            eid VARCHAR(64),
            loaded_at DATETIME
        )
        UNIQUE KEY(iccid)
        DISTRIBUTED BY HASH(iccid) BUCKETS 8
        PROPERTIES ("replication_num" = "1");
        """
    )

    execute(
        f"""
        CREATE TABLE IF NOT EXISTS {REJECTED_TABLE} (
            file_name VARCHAR(255),
            reject_reason VARCHAR(100),
            companyid VARCHAR(64),
            customernumber VARCHAR(64),
            iccid VARCHAR(64),
            raw_row TEXT,
            rejected_at DATETIME
        )
        DUPLICATE KEY(file_name, reject_reason, iccid)
        DISTRIBUTED BY HASH(file_name) BUCKETS 4
        PROPERTIES ("replication_num" = "1");
        """
    )


def is_already_loaded(file_name: str) -> bool:
    rows = execute(
        f"""
        SELECT COUNT(*)
        FROM {RAW_TABLE}
        WHERE file_name = %s
        """,
        (file_name,),
    )

    return rows[0][0] > 0


def normalize(value: str) -> str:
    return value.strip()


def reject_row(
    rejected_rows,
    file_name,
    reason,
    row,
):
    rejected_rows.append(
        (
            file_name,
            reason,
            row[0] if len(row) > 0 else "",
            row[11] if len(row) > 11 else "",
            row[21] if len(row) > 21 else "",
            ";".join(row),
            datetime.now(),
        )
    )


def process_file(
    minio,
    object_name: str,
):
    """
    Load one validated file.
    """

    file_name = os.path.basename(object_name)

    if is_already_loaded(file_name):
        print(
            f"Skipping already loaded file: {file_name}"
        )
        return 0, 0

    compressed = file_name.endswith(".csv.gz")

    with tempfile.TemporaryDirectory() as temp_dir:

        local_path = os.path.join(
            temp_dir,
            file_name,
        )

        download_object(
            minio,
            BUCKET_NAME,
            object_name,
            local_path,
        )

        if compressed:
            file_handle = gzip.open(
                local_path,
                "rt",
                encoding="utf-8",
                newline="",
            )
        else:
            file_handle = open(
                local_path,
                "r",
                encoding="utf-8",
                newline="",
            )

        loaded_rows = []
        rejected_rows = []

        try:
            reader = csv.reader(
                file_handle,
                delimiter=";",
            )

            header = next(reader)

            for row in reader:

                # Required:
                # companyid
                # customernumber
                # iccid

                company_id = normalize(row[0])
                customer_number = normalize(row[11])
                iccid = normalize(row[21])

                if not iccid:
                    reject_row(
                        rejected_rows,
                        file_name,
                        "NULL_OR_EMPTY_ICCID",
                        row,
                    )
                    continue

                if not customer_number:
                    reject_row(
                        rejected_rows,
                        file_name,
                        "NULL_OR_EMPTY_CUSTOMER_NUMBER",
                        row,
                    )
                    continue

                if not company_id or company_id == "0":
                    reject_row(
                        rejected_rows,
                        file_name,
                        "NULL_OR_ZERO_COMPANY_ID",
                        row,
                    )
                    continue

                loaded_rows.append(
                    (
                        file_name,
                        *row,
                        datetime.now(),
                    )
                )

        finally:
            file_handle.close()

        # -------------------------------------------------
        # De-duplicate rows by ICCID.
        #
        # Since UNIQUE KEY(iccid) is used by Doris,
        # the last row supplied for an ICCID wins.
        # -------------------------------------------------

        deduped = {}

        for row in loaded_rows:
            iccid = row[21]
            deduped[iccid] = row

        loaded_rows = list(deduped.values())

        # -------------------------------------------------
        # Insert valid rows
        # -------------------------------------------------

        if loaded_rows:

            placeholders = ", ".join(
                ["%s"] * len(RAW_COLUMNS)
            )

            query = f"""
                INSERT INTO {RAW_TABLE}
                ({", ".join(RAW_COLUMNS)})
                VALUES ({placeholders})
            """

            execute_many(
                query,
                loaded_rows,
            )

        # -------------------------------------------------
        # Insert rejected rows
        # -------------------------------------------------

        if rejected_rows:

            query = f"""
                INSERT INTO {REJECTED_TABLE}
                (
                    file_name,
                    reject_reason,
                    companyid,
                    customernumber,
                    iccid,
                    raw_row,
                    rejected_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            execute_many(
                query,
                rejected_rows,
            )

        print(
            f"{file_name}: "
            f"loaded={len(loaded_rows)}, "
            f"rejected={len(rejected_rows)}"
        )

        return (
            len(loaded_rows),
            len(rejected_rows),
        )


def load_validated_files():
    """
    Task 2:
    Read validated files and load them into Doris.
    """

    ensure_raw_tables()

    minio = get_minio_client()

    objects = list_files(
        minio,
        BUCKET_NAME,
        VALIDATED_PREFIX,
    )

    total_loaded = 0
    total_rejected = 0

    for obj in objects:

        object_name = obj.object_name

        if object_name.endswith("/"):
            continue

        try:

            loaded, rejected = process_file(
                minio,
                object_name,
            )

            total_loaded += loaded
            total_rejected += rejected

        except Exception as exc:

            print(
                f"ERROR loading {object_name}: {exc}"
            )

            # Per-file isolation:
            # one file should not stop the batch.
            continue

    print(
        "Task 2 complete: "
        f"loaded={total_loaded}, "
        f"rejected={total_rejected}"
    )