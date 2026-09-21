import os
import csv
import random
import uuid
import hashlib

from datetime import datetime, timedelta

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
# GENERATE NL PREACTIVE FEEDS
# =========================================================

def generate_nl_preactive_feed(

    feed_configs,

    sample_dir,

    output_dir
):

    print("\n===================================")
    print("GENERATING NL PREACTIVE FEEDS")
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
        # GENERATE FILES
        # =================================================

        for seq in range(

            1,

            config["files_per_day"] + 1
        ):

            file_time = (

                datetime.strptime(
                    feed_date,
                    "%Y%m%d"
                )

                +

                timedelta(

                    minutes=
                    config["interval_minutes"]
                    * (seq - 1)
                )
            )

            timestamp = file_time.strftime(
                "%Y%m%d%H%M%S"
            )

            file_name = (

                f"{config['feed_prefix']}_"

                f"{config['feed_sequence']}_"

                f"{timestamp}.csv"
            )

            output_file = os.path.join(
                output_dir,
                file_name
            )

            generated_rows = []

            # =============================================
            # GENERATE RECORDS
            # =============================================

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

            # =============================================
            # WRITE CSV
            # =============================================

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

            # =============================================
            # GENERATE CHECKSUM
            # =============================================

            checksum_file = (
                output_file + ".CHKSUM"
            )

            sha256_hash = hashlib.sha256()

            with open(
                output_file,
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
            ) as chk:

                chk.write(checksum)

            print(
                f"Generated: "
                f"{os.path.basename(checksum_file)}"
            )

            # =============================================
            # UPLOAD CSV
            # =============================================

            hook.load_file(

                filename=output_file,

                key=f"{MINIO_FOLDER}/{file_name}",

                bucket_name=MINIO_BUCKET,

                replace=True
            )

            # =============================================
            # UPLOAD CHKSUM
            # =============================================

            hook.load_file(

                filename=checksum_file,

                key=f"{MINIO_FOLDER}/{os.path.basename(checksum_file)}",

                bucket_name=MINIO_BUCKET,

                replace=True
            )

            print(
                f"Uploaded to MinIO"
            )

            # =============================================
            # CLEANUP
            # =============================================

            os.remove(output_file)

            os.remove(checksum_file)

            total_files_generated += 1

    print("\n===================================")
    print("NL PREACTIVE SUMMARY")
    print("===================================\n")

    print(
        f"TOTAL FILES GENERATED : "
        f"{total_files_generated}"
    )

    print(
        "\nNL PREACTIVE FEED "
        "GENERATION COMPLETED\n"
    )