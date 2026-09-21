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


def generate_uuid():

    return str(
        uuid.uuid4()
    ).upper()

# =========================================================
# GENERATE PRODUCT SUBSCRIPTION FEED
# =========================================================

def generate_product_subscription_feed(

    feed_configs,

    sample_dir,

    output_dir
):

    print("\n===================================")
    print("GENERATING PRODUCT SUBSCRIPTION")
    print("===================================\n")

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    feed_date = datetime.now().strftime(
        "%Y%m%d"
    )

    total_files_generated = 0

    for config in feed_configs:

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

        header = rows[0]

        sample_data = rows[1:]

        header_index = {

            col.strip().lower(): idx
            for idx, col in enumerate(header)
        }

        # =================================================
        # FILE NAME
        # =================================================

        timestamp = (
            feed_date + "080007"
        )

        file_name = (

            f"{config['feed_prefix']}_"

            f"{config['feed_pattern']}_"

            f"{config['feed_sequence']}_"

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

                col_lower = col.lower()

                if "iccid" in col_lower:

                    row[idx] = generate_iccid()

                elif "imsi" in col_lower:

                    row[idx] = generate_imsi()

                elif "msisdn" in col_lower:

                    row[idx] = generate_msisdn()

                elif "uuid" in col_lower:

                    row[idx] = generate_uuid()

            generated_rows.append(row)

        # =================================================
        # WRITE CSV.GZ
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
        # UPLOAD
        # =================================================

        hook.load_file(

            filename=output_file,

            key=f"{MINIO_FOLDER}/{file_name}",

            bucket_name=MINIO_BUCKET,

            replace=True
        )

        print(
            f"Uploaded to MinIO"
        )

        # =================================================
        # CLEANUP
        # =================================================

        os.remove(output_file)

        total_files_generated += 1

    print("\n===================================")
    print("PRODUCT SUBSCRIPTION SUMMARY")
    print("===================================\n")

    print(
        f"TOTAL FILES GENERATED : "
        f"{total_files_generated}"
    )

    print(
        "\nPRODUCT SUBSCRIPTION "
        "GENERATION COMPLETED\n"
    )