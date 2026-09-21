import os
import csv
import gzip
import random
import uuid

from datetime import datetime, timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

AWS_CONN_ID = "minio_load_feed_simulator"
MINIO_BUCKET = "crushftp"
MINIO_FOLDER = "mediation/landing"


def generate_iccid():

    return (
        "893107"
        + str(random.randint(100000000000, 999999999999))
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
        + str(random.randint(80000000, 99999999))
    )


def generate_uuid():

    return str(uuid.uuid4()).upper()


def generate_email():

    return (
        f"user{random.randint(1000,9999)}@gmail.com"
    )


def generate_active_feed(
    feed_configs,
    sample_dir,
    output_dir
):

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    feed_summary = {}

    for config in feed_configs:

        generated_count = 0

        sample_file = os.path.join(
            sample_dir,
            config["sample_file"]
        )

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

        header = rows[0]
        sample_data = rows[1:]

        header_index = {
            col.strip().lower(): idx
            for idx, col in enumerate(header)
        }

        for seq in range(
            1,
            config["files_per_day"] + 1
        ):

            file_time = (
                datetime.now().replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0
                )
                + timedelta(
                    minutes=config["interval_minutes"]
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

            output_file = os.path.join(
                output_dir,
                file_name
            )

            generated_rows = []

            for _ in range(
                config["records_per_file"]
            ):

                row = random.choice(
                    sample_data
                ).copy()

                for col in header_index:

                    idx = header_index[col]

                    if idx >= len(row):
                        continue

                    if "iccid" in col:

                        row[idx] = generate_iccid()

                    elif "imsi" in col:

                        row[idx] = generate_imsi()

                    elif "msisdn" in col:

                        row[idx] = generate_msisdn()

                    elif "uuid" in col:

                        row[idx] = generate_uuid()

                    elif "email" in col:

                        row[idx] = generate_email()

                generated_rows.append(row)

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
                writer.writerows(generated_rows)

            hook.load_file(
                filename=output_file,
                key=f"{MINIO_FOLDER}/{file_name}",
                bucket_name=MINIO_BUCKET,
                replace=True
            )

            os.remove(output_file)

            generated_count += 1

        feed_summary[
            config["feed_prefix"]
        ] = generated_count

    print("\nACTIVE FEED SUMMARY\n")

    for feed, count in feed_summary.items():

        print(f"{feed} : {count} files")