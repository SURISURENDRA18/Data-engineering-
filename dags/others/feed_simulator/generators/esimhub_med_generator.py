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

    return (
        str(uuid.uuid4())
        .replace("-", "")
        .upper()
    )

# =========================================================
# CREATE SHA256 CHECKSUM
# =========================================================

def generate_checksum_file(file_path):

    checksum_file = (
        file_path + ".cksum"
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
# GENERATE ESIMHUB MED FEED
# =========================================================

def generate_esimhub_med_feed(

    feed_configs,

    sample_dir,

    output_dir
):

    print("\n===================================")
    print("GENERATING ESIMHUB MED FEEDS")
    print("===================================\n")

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
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
        # MAIN FEED FILE
        # =================================================

        timestamp = datetime.now().strftime(
            "%Y%m%d0000"
        )

        file_name = (
            f"eSIMHUB_MED_{timestamp}.csv"
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
                # ICCID
                # =========================================

                if "iccid" in col_lower:

                    row[idx] = generate_iccid()

                # =========================================
                # IMSI
                # =========================================

                elif "imsi" in col_lower:

                    row[idx] = generate_imsi()

                # =========================================
                # MSISDN
                # =========================================

                elif "msisdn" in col_lower:

                    row[idx] = generate_msisdn()

                # =========================================
                # UUID
                # =========================================

                elif "uuid" in col_lower:

                    row[idx] = generate_uuid()

            generated_rows.append(row)

        # =================================================
        # WRITE MAIN FEED FILE
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
            f"Generated main feed: "
            f"{file_name}"
        )

        # =================================================
        # GENERATE CHECKSUM FILE
        # =================================================

        checksum_file = generate_checksum_file(
            output_file
        )

        print(
            f"Generated checksum: "
            f"{os.path.basename(checksum_file)}"
        )

        # =================================================
        # DAILY RECON FILE
        # =================================================

        daily_recon_name = (
            f"DailyRecon_eSIMHUB_MED_{timestamp}.csv"
        )

        daily_recon_file = os.path.join(
            output_dir,
            daily_recon_name
        )

        with open(

            daily_recon_file,

            "w",

            encoding="utf-8",

            newline=''
        ) as recon_csv:

            writer = csv.writer(

                recon_csv,

                delimiter=";"
            )

            writer.writerow([

                "File_Name",

                "Record_count"
            ])

            writer.writerow([

                file_name,

                len(generated_rows)
            ])

        print(
            f"Generated daily recon: "
            f"{daily_recon_name}"
        )

        # =================================================
        # MONTHLY RECON FILE
        # =================================================

        monthly_recon_name = (
            f"MonthlyRecon_eSIMHUB_MED_{timestamp}.csv"
        )

        monthly_recon_file = os.path.join(
            output_dir,
            monthly_recon_name
        )

        with open(

            monthly_recon_file,

            "w",

            encoding="utf-8",

            newline=''
        ) as recon_csv:

            writer = csv.writer(

                recon_csv,

                delimiter=";"
            )

            writer.writerow([

                "File_Name",

                "Record_count"
            ])

            writer.writerow([

                file_name,

                len(generated_rows)
            ])

        print(
            f"Generated monthly recon: "
            f"{monthly_recon_name}"
        )

        # =================================================
        # UPLOAD MAIN FEED
        # =================================================

        hook.load_file(

            filename=output_file,

            key=f"{MINIO_FOLDER}/{file_name}",

            bucket_name=MINIO_BUCKET,

            replace=True
        )

        # =================================================
        # UPLOAD CHECKSUM
        # =================================================

        hook.load_file(

            filename=checksum_file,

            key=f"{MINIO_FOLDER}/{os.path.basename(checksum_file)}",

            bucket_name=MINIO_BUCKET,

            replace=True
        )

        # =================================================
        # UPLOAD DAILY RECON
        # =================================================

        hook.load_file(

            filename=daily_recon_file,

            key=f"{MINIO_FOLDER}/{daily_recon_name}",

            bucket_name=MINIO_BUCKET,

            replace=True
        )

        # =================================================
        # UPLOAD MONTHLY RECON
        # =================================================

        hook.load_file(

            filename=monthly_recon_file,

            key=f"{MINIO_FOLDER}/{monthly_recon_name}",

            bucket_name=MINIO_BUCKET,

            replace=True
        )

        print(
            "Uploaded all files to MinIO"
        )

        # =================================================
        # CLEANUP
        # =================================================

        try:

            os.remove(output_file)

            os.remove(checksum_file)

            os.remove(daily_recon_file)

            os.remove(monthly_recon_file)

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
    print("ESIMHUB MED SUMMARY")
    print("===================================\n")

    print(
        f"TOTAL FILES GENERATED : "
        f"{total_files_generated}"
    )

    print(
        "\nESIMHUB MED GENERATION COMPLETED\n"
    )