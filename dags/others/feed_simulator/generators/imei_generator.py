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

def generate_imei():

    tac = str(
        random.randint(
            10000000,
            99999999
        )
    )

    serial = str(
        random.randint(
            100000,
            999999
        )
    )

    check = str(
        random.randint(
            0,
            9
        )
    )

    return tac + serial + check


def generate_uuid():

    return (
        str(uuid.uuid4())
        .replace("-", "")
        .upper()
    )

# =========================================================
# GENERATE IMEI FEED
# =========================================================

def generate_imei_feed(

    feed_configs,

    sample_dir,

    output_dir
):

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    feed_summary = {}

    # =====================================================
    # PROCESS EACH CONFIG
    # =====================================================

    for config in feed_configs:

        generated_count = 0

        sample_file = os.path.join(

            sample_dir,

            config["sample_file"]
        )

        # =================================================
        # READ SAMPLE FILE
        # =================================================

        with open(

            sample_file,

            "r",

            encoding="utf-8"
        ) as f:

            reader = csv.reader(

                f,

                delimiter=config["delimiter"]
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
        # GENERATE FILE
        # =================================================

        timestamp = (
            datetime.now().strftime(
                "%Y%m%d"
            )
            + "230008"
        )

        file_name = (

            f"{config['feed_prefix']}_"

            f"{config['feed_pattern']}_"

            f"000000001_"

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

            if len(row) < len(header):

                row.extend(

                    ['']
                    * (
                        len(header)
                        - len(row)
                    )
                )

            for col in header_index:

                idx = header_index[col]

                if idx >= len(row):

                    continue

                col_lower = (
                    col.strip().lower()
                )

                # =========================================
                # IMEI
                # =========================================

                if "imei" in col_lower:

                    row[idx] = generate_imei()

                # =========================================
                # UUID
                # =========================================

                elif "uuid" in col_lower:

                    row[idx] = generate_uuid()

            generated_rows.append(row)

        # =================================================
        # WRITE OUTPUT FILE
        # =================================================

        with gzip.open(

            output_file,

            "wt",

            encoding="utf-8",

            newline=""
        ) as gzfile:

            writer = csv.writer(

                gzfile,

                delimiter=config["delimiter"]
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

        os.remove(output_file)

        generated_count += 1

        feed_summary[
            config["feed_prefix"]
        ] = generated_count

    # =====================================================
    # FINAL SUMMARY
    # =====================================================

    print("\n===================================")
    print("IMEI FEED SUMMARY")
    print("===================================\n")

    for feed, count in feed_summary.items():

        print(
            f"{feed} : {count} files"
        )

    print(
        "\nIMEI FEED GENERATION COMPLETED\n"
    )