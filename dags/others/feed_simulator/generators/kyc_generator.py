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
# GENERATE KYC FEEDS
# =========================================================

def generate_kyc_feed(

    feed_configs,

    sample_dir,

    output_dir
):

    print("\n===================================")
    print("GENERATING KYC FEEDS")
    print("===================================\n")

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    feed_date = datetime.now().strftime(
        "%Y%m%d"
    )

    # =====================================================
    # SUMMARY
    # =====================================================

    feed_summary = {}

    total_files_generated = 0

    # =====================================================
    # LOOP THROUGH ALL KYC FEEDS
    # =====================================================

    for config in feed_configs:

        print(
            f"\nProcessing: "
            f"{config['feed_prefix']}"
        )

        generated_file_count = 0

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
                f"{config['feed_prefix']}"
                f"_{timestamp}.csv"
            )

            output_file = os.path.join(
                output_dir,
                file_name
            )

            generated_rows = []

            # =============================================
            # GENERATE RECORDS
            # =============================================

            for rec in range(
                config["records_per_file"]
            ):

                row = random.choice(
                    sample_data
                ).copy()

                # =========================================
                # NORMALIZE ROW LENGTH
                # =========================================

                if len(row) < len(header):

                    row.extend(

                        ['']
                        * (
                            len(header)
                            - len(row)
                        )
                    )

                try:

                    for col in header_index:

                        idx = header_index[col]

                        if idx >= len(row):

                            continue

                        col_lower = col.lower()

                        # =================================
                        # CUSTOMER ID
                        # =================================

                        if (
                            "customer" in col_lower
                            and
                            "id" in col_lower
                        ):

                            row[idx] = (

                                "CUST"

                                + str(
                                    random.randint(
                                        100000,
                                        999999
                                    )
                                )
                            )

                        # =================================
                        # EMAIL
                        # =================================

                        elif "email" in col_lower:

                            row[idx] = (

                                f"user"
                                f"{random.randint(1000,9999)}"
                                f"@gmail.com"
                            )

                        # =================================
                        # MSISDN
                        # =================================

                        elif "msisdn" in col_lower:

                            row[idx] = (

                                "65"

                                + str(
                                    random.randint(
                                        80000000,
                                        99999999
                                    )
                                )
                            )

                        # =================================
                        # NAME
                        # =================================

                        elif "name" in col_lower:

                            first_names = [

                                "John",
                                "David",
                                "Sara",
                                "Emma",
                                "Chris"
                            ]

                            last_names = [

                                "Smith",
                                "Brown",
                                "Taylor",
                                "Wilson",
                                "Lee"
                            ]

                            row[idx] = (

                                random.choice(
                                    first_names
                                )

                                + " "

                                + random.choice(
                                    last_names
                                )
                            )

                        # =================================
                        # UUID
                        # =================================

                        elif "uuid" in col_lower:

                            row[idx] = str(
                                uuid.uuid4()
                            ).upper()

                    # =====================================
                    # RANDOM DATE/TIME
                    # =====================================

                    random_date = (

                        datetime.strptime(
                            feed_date,
                            "%Y%m%d"
                        )

                        +

                        timedelta(

                            seconds=random.randint(
                                0,
                                86399
                            )
                        )
                    )

                    for col in header_index:

                        idx = header_index[col]

                        if idx >= len(row):

                            continue

                        col_lower = col.lower()

                        if "date" in col_lower:

                            row[idx] = (
                                random_date.strftime(
                                    "%Y%m%d"
                                )
                            )

                        elif "time" in col_lower:

                            row[idx] = (
                                random_date.strftime(
                                    "%H:%M:%S"
                                )
                            )

                except Exception as e:

                    print(
                        f"Randomization warning: "
                        f"{e}"
                    )

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
            # CREATE SHA256 FILE
            # =============================================

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

            sha_file_name = (
                file_name + ".sha256"
            )

            sha_file_path = os.path.join(

                output_dir,

                sha_file_name
            )

            with open(
                sha_file_path,
                "w"
            ) as sha_file:

                sha_file.write(checksum)

            print(
                f"Generated: {sha_file_name}"
            )

            # =============================================
            # UPLOAD CSV
            # =============================================

            object_name = (
                f"{MINIO_FOLDER}/{file_name}"
            )

            hook.load_file(

                filename=output_file,

                key=object_name,

                bucket_name=MINIO_BUCKET,

                replace=True
            )

            print(
                f"Uploaded: {object_name}"
            )

            # =============================================
            # UPLOAD SHA256
            # =============================================

            sha_object_name = (
                f"{MINIO_FOLDER}/{sha_file_name}"
            )

            hook.load_file(

                filename=sha_file_path,

                key=sha_object_name,

                bucket_name=MINIO_BUCKET,

                replace=True
            )

            print(
                f"Uploaded: {sha_object_name}"
            )

            # =============================================
            # CLEANUP
            # =============================================

            try:

                os.remove(output_file)

                os.remove(sha_file_path)

                print(
                    f"Cleaned up: "
                    f"{file_name}"
                )

            except Exception as e:

                print(
                    f"Cleanup failed: {e}"
                )

            generated_file_count += 1

            total_files_generated += 1

        # =================================================
        # FEED SUMMARY
        # =================================================

        feed_summary[
            config["feed_prefix"]
        ] = generated_file_count

    # =====================================================
    # FINAL SUMMARY
    # =====================================================

    print("\n===================================")
    print("KYC FEED SUMMARY")
    print("===================================\n")

    for feed, count in feed_summary.items():

        print(
            f"{feed} : "
            f"{count} files generated"
        )

    print(
        f"\nTOTAL FILES GENERATED : "
        f"{total_files_generated}"
    )

    print("\nKYC FEED GENERATION COMPLETED\n")