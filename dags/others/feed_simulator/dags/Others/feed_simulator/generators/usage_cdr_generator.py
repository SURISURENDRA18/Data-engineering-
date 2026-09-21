import os
import csv
import gzip
import random
import uuid

from datetime import datetime, timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# =========================================================
# MINIO CONFIGURATION
# =========================================================

AWS_CONN_ID = "minio_load_feed_simulator"

MINIO_BUCKET = "crushftp"

MINIO_FOLDER = "mediation/landing"

# =========================================================
# GENERATE USAGE CDR FEEDS
# =========================================================

def generate_usage_cdr_feed(

    feed_configs,

    sample_dir,

    output_dir
):

    print("\n===================================")
    print("GENERATING USAGE CDR FEEDS")
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
    # LOOP THROUGH ALL USAGE CDR FEEDS
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

            seq_no = str(seq).zfill(9)

            file_name = (

                f"{config['feed_prefix']}_"

                f"{config['feed_pattern']}_"

                f"{seq_no}_"

                f"{timestamp}.csv.gz"
            )

            sem_name = file_name.replace(
                ".csv.gz",
                ".SEM"
            )

            output_file = os.path.join(
                output_dir,
                file_name
            )

            sem_file = os.path.join(
                output_dir,
                sem_name
            )

            generated_rows = []

            # =============================================
            # PRE-GENERATE DUPLICATE CDR ID POOL
            # =============================================

            duplicate_pool = []

            duplicate_pool_size = random.randint(
                20,
                30
            )

            for _ in range(duplicate_pool_size):

                duplicate_pool.append(

                    str(uuid.uuid4())
                    .replace("-", "")
                    .upper()
                )

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

                        col_lower = col.strip().lower()

                        # =================================
                        # CDR ID
                        # =================================

                        if (

                            "cdrid" in col_lower

                            or

                            "cdr_id" in col_lower
                        ):

                            # 90% unique
                            # 10% duplicates

                            if random.random() < 0.10:

                                row[idx] = random.choice(
                                    duplicate_pool
                                )

                            else:

                                row[idx] = (

                                    str(uuid.uuid4())
                                    .replace("-", "")
                                    .upper()
                                )

                        # =================================
                        # IMSI
                        # =================================

                        elif "imsi" in col_lower:

                            row[idx] = str(

                                random.randint(
                                    404000000000000,
                                    404999999999999
                                )
                            )

                        # =================================
                        # MSISDN
                        # =================================

                        elif (

                            "msisdn"
                            in col_lower

                            or

                            "mobile"
                            in col_lower
                        ):

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
                        # UUID
                        # =================================

                        elif "uuid" in col_lower:

                            row[idx] = (

                                str(uuid.uuid4())
                                .replace("-", "")
                                .upper()
                            )

                        # =================================
                        # IMEI
                        # =================================

                        elif "imei" in col_lower:

                            row[idx] = str(

                                random.randint(
                                    100000000000000,
                                    999999999999999
                                )
                            )

                        # =================================
                        # ICCID / SIM
                        # =================================

                        elif (

                            "iccid" in col_lower

                            or

                            "sim" == col_lower
                        ):

                            row[idx] = str(

                                random.randint(
                                    8901000000000000000,
                                    8901999999999999999
                                )
                            )

                        # =================================
                        # DATE
                        # =================================

                        elif "date" in col_lower:

                            row[idx] = (

                                random_date.strftime(
                                    "%Y%m%d"
                                )
                            )

                        # =================================
                        # TIME
                        # =================================

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
            # WRITE CSV.GZ
            # =============================================

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

            # =============================================
            # WRITE SEM FILE
            # =============================================

            with open(
                sem_file,
                "w"
            ) as sem:

                sem.write(
                    str(
                        config["records_per_file"]
                    )
                )

            print(
                f"Generated: {sem_name}"
            )

            # =============================================
            # UPLOAD CSV.GZ
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
            # UPLOAD SEM FILE
            # =============================================

            sem_object_name = (
                f"{MINIO_FOLDER}/{sem_name}"
            )

            hook.load_file(

                filename=sem_file,

                key=sem_object_name,

                bucket_name=MINIO_BUCKET,

                replace=True
            )

            print(
                f"Uploaded: {sem_object_name}"
            )

            # =============================================
            # CLEANUP
            # =============================================

            try:

                os.remove(output_file)

                os.remove(sem_file)

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
    print("USAGE CDR FEED SUMMARY")
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

    print("\nUSAGE CDR FEED GENERATION COMPLETED\n")