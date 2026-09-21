import os
import csv
import gzip
import random
import uuid

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


def generate_imsi():

    return str(
        random.randint(
            404000000000000,
            404999999999999
        )
    )

# =========================================================
# GENERATE MM REPORT FEED
# =========================================================

def generate_mm_report_feed(

    feed_configs,

    sample_dir,

    output_dir
):

    print("\n===================================")
    print("GENERATING MM REPORT FEEDS")
    print("===================================\n")

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    feed_date = datetime.now().strftime(
        "%Y%m%d"
    )

    total_files_generated = 0

    # =====================================================
    # PROCESS EACH CONFIG
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
        # LOAD SAMPLE CSV FILE
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
        # GENERATE OUTPUT FILE NAME
        # =================================================

        timestamp = (
            feed_date + "080008"
        )

        seq_no = config.get(
            "feed_sequence",
            "000000001"
        )

        file_name = (

            f"{config['feed_prefix']}_"

            f"{config['feed_pattern']}_"

            f"{seq_no}_"

            f"{timestamp}.csv.gz"
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
                # UUID FIELDS
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
                # SPONSOR / CUSTOMER IDs
                # =========================================

                elif (

                    "sponsor" in col_lower

                    or

                    "customer" in col_lower
                ):

                    row[idx] = str(

                        random.randint(
                            100000,
                            999999
                        )
                    )

            generated_rows.append(row)

        # =================================================
        # WRITE OUTPUT GZIP FILE
        # =================================================

        with gzip.open(

            output_file,

            "wt",

            encoding="utf-8",

            newline=''
        ) as gzfile:

            writer = csv.writer(

                gzfile,

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
        # UPLOAD TO MINIO
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
        # CLEANUP
        # =================================================

        try:

            os.remove(output_file)

            print(
                "Cleaned up local file"
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
    print("MM REPORT SUMMARY")
    print("===================================\n")

    print(
        f"TOTAL FILES GENERATED : "
        f"{total_files_generated}"
    )

    print(
        "\nMM REPORT GENERATION COMPLETED\n"
    )