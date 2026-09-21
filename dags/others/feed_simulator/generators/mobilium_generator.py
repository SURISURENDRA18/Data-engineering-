import os
import csv
import random
import uuid
import hashlib

from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# =========================================================
# MINIO CONFIGURATION
# =========================================================

AWS_CONN_ID = "minio_load_feed_simulator"

MINIO_BUCKET = "crushftp"

MINIO_FOLDER = "mediation/landing"

# =========================================================
# RANDOM GENERATORS
# =========================================================

def generate_uuid():

    return (
        str(uuid.uuid4())
        .replace("-", "")
        .upper()
    )


def generate_imsi():

    return str(
        random.randint(
            404000000000000,
            404999999999999
        )
    )


def generate_msisdn():

    return (
        "65"
        + str(
            random.randint(
                80000000,
                99999999
            )
        )
    )


def generate_iccid():

    return (
        "893107"
        + str(
            random.randint(
                100000000000,
                999999999999
            )
        )
    )

# =========================================================
# CREATE CHECKSUM FILE
# =========================================================

def generate_checksum_file(

    file_path,

    checksum_extension=".cksum"
):

    checksum_file = (
        file_path.replace(
            ".csv",
            checksum_extension
        )
    )

    sha256_hash = hashlib.sha256()

    with open(
        file_path,
        "rb"
    ) as f:

        for chunk in iter(
            lambda: f.read(4096),
            b""
        ):

            sha256_hash.update(chunk)

    checksum = sha256_hash.hexdigest()

    with open(
        checksum_file,
        "w"
    ) as cksum:

        cksum.write(

            f"{checksum} "
            f"{os.path.basename(file_path)}"
        )

    return checksum_file

# =========================================================
# GENERATE MOBILIUM FEED
# =========================================================

def generate_mobilium_feed(

    feed_configs,

    sample_dir,

    output_dir
):

    print("\n===================================")
    print("GENERATING MOBILIUM FEEDS")
    print("===================================\n")

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    total_files_generated = 0

    # =====================================================
    # PROCESS CONFIGS
    # =====================================================

    for config in feed_configs:

        print(
            f"\nProcessing: "
            f"{config['feed_prefix']}"
        )

        sample_file = os.path.join(

            sample_dir,

            config["sample_file"]
        )

        delimiter = config["delimiter"]

        # =================================================
        # LOAD SAMPLE FILE
        # =================================================

        with open(
            sample_file,
            "r",
            encoding="utf-8"
        ) as f:

            reader = csv.reader(
                f,
                delimiter=delimiter
            )

            rows = list(reader)

        if len(rows) == 0:

            raise Exception(
                f"Empty sample file: "
                f"{sample_file}"
            )

        header = rows[0]

        sample_data = rows[1:]

        header_index = {

            col.strip().lower(): idx
            for idx, col in enumerate(header)
        }

        # =================================================
        # GENERATE FILE NAME
        # =================================================

        timestamp = datetime.now().strftime(
            "%Y%m%d0500"
        )

        file_name = (
            f"{config['feed_prefix']}_"
            f"{timestamp}_001.csv"
        )

        output_file = os.path.join(
            output_dir,
            file_name
        )

        generated_rows = []

        # =================================================
        # GENERATE RECORDS
        # =================================================

        for _ in range(
            config["records_per_file"]
        ):

            row = random.choice(
                sample_data
            ).copy()

            # =============================================
            # NORMALIZE ROW LENGTH
            # =============================================

            if len(row) < len(header):

                row.extend(

                    ['']
                    * (
                        len(header)
                        - len(row)
                    )
                )

            # =============================================
            # RANDOMIZE DATA
            # =============================================

            for col in header_index:

                idx = header_index[col]

                if idx >= len(row):

                    continue

                col_lower = (
                    col.strip().lower()
                )

                # =========================================
                # UUID
                # =========================================

                if "uuid" in col_lower:

                    row[idx] = generate_uuid()

                # =========================================
                # IMSI
                # =========================================

                elif "imsi" in col_lower:

                    row[idx] = generate_imsi()

                # =========================================
                # MSISDN
                # =========================================

                elif (

                    "msisdn" in col_lower

                    or

                    "mobile" in col_lower
                ):

                    row[idx] = generate_msisdn()

                # =========================================
                # ICCID
                # =========================================

                elif "iccid" in col_lower:

                    row[idx] = generate_iccid()

            generated_rows.append(row)

        # =================================================
        # WRITE CSV FILE
        # =================================================

        with open(

            output_file,

            "w",

            encoding="utf-8",

            newline=''
        ) as csvfile:

            writer = csv.writer(

                csvfile,

                delimiter=delimiter
            )

            writer.writerow(header)

            writer.writerows(
                generated_rows
            )

        print(
            f"Generated: {file_name}"
        )

        # =================================================
        # GENERATE CHECKSUM FILE
        # =================================================

        checksum_file = generate_checksum_file(

            output_file,

            config.get(
                "checksum_extension",
                ".cksum"
            )
        )

        print(
            f"Generated checksum: "
            f"{os.path.basename(checksum_file)}"
        )

        # =================================================
        # UPLOAD MAIN FILE
        # =================================================

        hook.load_file(

            filename=output_file,

            key=f"{MINIO_FOLDER}/{file_name}",

            bucket_name=MINIO_BUCKET,

            replace=True
        )

        print(
            f"Uploaded: {file_name}"
        )

        # =================================================
        # UPLOAD CHECKSUM FILE
        # =================================================

        hook.load_file(

            filename=checksum_file,

            key=f"{MINIO_FOLDER}/{os.path.basename(checksum_file)}",

            bucket_name=MINIO_BUCKET,

            replace=True
        )

        print(
            f"Uploaded checksum: "
            f"{os.path.basename(checksum_file)}"
        )

        # =================================================
        # CLEANUP
        # =================================================

        try:

            os.remove(output_file)

            os.remove(checksum_file)

            print(
                "Cleaned up local files"
            )

        except Exception as e:

            print(
                f"Cleanup failed: {e}"
            )

        total_files_generated += 1

    # =====================================================
    # FINAL SUMMARY
    # =====================================================

    print("\n===================================")
    print("MOBILIUM SUMMARY")
    print("===================================\n")

    print(
        f"TOTAL FILES GENERATED : "
        f"{total_files_generated}"
    )

    print(
        "\nMOBILIUM GENERATION COMPLETED\n"
    )