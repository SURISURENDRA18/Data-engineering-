import csv
import io
import os
import random
import uuid

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# =========================================================
# MINIO CONFIGURATION
# =========================================================

AWS_CONN_ID = "minio_load_feed_simulator"

MINIO_BUCKET = "crushftp"

# Reference/seed dataset
REFERENCE_OBJECT = (
    "data/reference/dataset_motor_insurance_policy.csv"
)

# Generated source feeds
SOURCE_ROOT = "data/source"

POLICY_FOLDER = f"{SOURCE_ROOT}/policy"
CLAIM_FOLDER = f"{SOURCE_ROOT}/claim"


# =========================================================
# GENERATOR CONFIGURATION
# =========================================================

# One DAG execution = one source batch.
# Schedule the DAG every 5 minutes.
FILES_PER_RUN = 1

POLICY_RECORDS_PER_FILE = 50
CLAIM_RECORDS_PER_FILE = 10

# Optional development limit. None = load entire reference CSV.
REFERENCE_SAMPLE_SIZE =  500


# =========================================================
# CSV SCHEMAS
# =========================================================

POLICY_COLUMNS = [
    "policy_id",
    "policy_number",
    "customer_id",
    "product_id",
    "insured_id",
    "year",
    "policy_type",
    "policy_status",
    "business_type",
    "payment_frequency",
    "bonus_score",
    "driver_age",
    "vehicle_age",
    "age_driving_licence",
    "fuel_type",
    "vehicle_value",
    "seats",
    "power_to_weight_ratio",
    "vehicle_brand",
    "municipality_type",
    "circulation_area",
    "total_premium",
    "liability_premium",
    "property_damage_premium",
    "theft_premium",
    "fire_premium",
    "glass_premium",
    "legal_protection_premium",
    "occupants_premium",
    "total_exposure",
    "liability_exposure",
    "policy_start_date",
    "policy_end_date",
    "event_type",
    "event_timestamp",
]

CLAIM_COLUMNS = [
    "claim_id",
    "claim_number",
    "policy_id",
    "customer_id",
    "insured_id",
    "claim_type",
    "claim_status",
    "claim_date",
    "incident_date",
    "settlement_date",
    "claimed_amount",
    "approved_amount",
    "settled_amount",
    "event_type",
    "event_timestamp",
]


# =========================================================
# HELPERS
# =========================================================

def clean(value: str) -> str:
    return (value or "").strip()


def safe_int(value: str, default: int = 0) -> int:
    try:
        return int(float(clean(value)))
    except (TypeError, ValueError):
        return default


def safe_float(value: str, default: float = 0.0) -> float:
    try:
        return float(clean(value))
    except (TypeError, ValueError):
        return default


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16].upper()}"


def money(value: float) -> str:
    return f"{max(value, 0.0):.2f}"


def timestamp_string(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def normalize_reference_row(
    row: Dict[str, str],
) -> Dict[str, str]:
    """
    Normalize reference CSV column names and values.
    This is only for the generator's input handling.
    Bronze remains responsible for actual pipeline transformations.
    """
    return {
        clean(key).lower(): clean(value)
        for key, value in row.items()
    }


# =========================================================
# LOAD REFERENCE DATA FROM MINIO
# =========================================================

def load_reference_data(
    hook: S3Hook,
    sample_size: Optional[int] = 500,
) -> List[Dict[str, str]]:
    """Stream the reference CSV and retain only a bounded sample."""
    print(f"Reading reference dataset: s3://{MINIO_BUCKET}/{REFERENCE_OBJECT}")

    key = hook.get_key(key=REFERENCE_OBJECT, bucket_name=MINIO_BUCKET)
    if key is None:
        raise FileNotFoundError(
            f"Reference dataset was not found: s3://{MINIO_BUCKET}/{REFERENCE_OBJECT}"
        )

    if sample_size is None or sample_size <= 0:
        raise ValueError("sample_size must be a positive integer")

    required_columns = {
        "insured_id", "year", "policy_type", "policy_status",
        "business_type", "payment_frequency", "driver_age", "vehicle_age",
        "fuel_type", "vehicle_value", "vehicle_brand", "total_premium",
        "total_claims", "total_incurred", "total_exposure",
    }

    body = None
    text_stream = None
    try:
        body = key.get()["Body"]
        text_stream = io.TextIOWrapper(body, encoding="utf-8-sig", newline="")

        reader = csv.DictReader(
            text_stream,
            delimiter=";",
            quotechar='"',
        )

        actual_columns = {
            clean(column).lower()
            for column in (reader.fieldnames or [])
            if column is not None
        }

        missing_columns = sorted(required_columns - actual_columns)
        if missing_columns:
            raise ValueError(
                "Reference dataset is missing required columns: "
                + ", ".join(missing_columns)
            )

        # Reservoir sampling: scan the source once, but retain at most
        # `sample_size` rows in memory regardless of source size.
        reservoir: List[Dict[str, str]] = []
        row_count = 0

        for row in reader:
            if not row:
                continue

            normalized = normalize_reference_row(row)
            if not any(normalized.values()):
                continue

            row_count += 1

            if len(reservoir) < sample_size:
                reservoir.append(normalized)
            else:
                replacement_index = random.randrange(row_count)
                if replacement_index < sample_size:
                    reservoir[replacement_index] = normalized

        if not reservoir:
            raise ValueError("Reference dataset contains no data rows.")

        print(
            f"Scanned {row_count} reference records; "
            f"retained {len(reservoir)} seed records in memory."
        )
        return reservoir

    finally:
        if text_stream is not None:
            text_stream.close()
        elif body is not None:
            body.close()


# =========================================================
# POLICY GENERATOR
# =========================================================

def generate_policy_event(
    seed: Dict[str, str],
    event_timestamp: datetime,
) -> Dict[str, str]:

    policy_start_date = event_timestamp.date()

    policy_end_date = (
        policy_start_date + timedelta(days=365)
    )

    premium_multiplier = random.uniform(
        0.95,
        1.05,
    )

    total_premium = (
        safe_float(seed.get("total_premium"))
        * premium_multiplier
    )

    return {
        "policy_id": new_id("POL"),

        "policy_number": (
            f"Policy-{event_timestamp:%Y%m%d}-"
            f"{uuid.uuid4().hex[:10].upper()}"
        ),

        "customer_id": new_id("CUS"),
        "product_id": new_id("PROD"),

        "insured_id": seed.get(
            "insured_id",
            "",
        ),

        "year": str(
            safe_int(seed.get("year"))
        ),

        "policy_type": seed.get(
            "policy_type",
            "",
        ),

        "policy_status": seed.get(
            "policy_status",
            "",
        ),

        "business_type": seed.get(
            "business_type",
            "",
        ),

        "payment_frequency": seed.get(
            "payment_frequency",
            "",
        ),

        "bonus_score": str(
            safe_int(seed.get("bonus_score"))
        ),

        "driver_age": str(
            safe_int(seed.get("driver_age"))
        ),

        "vehicle_age": str(
            safe_int(seed.get("vehicle_age"))
        ),

        "age_driving_licence": str(
            safe_int(
                seed.get("age_driving_licence")
            )
        ),

        "fuel_type": seed.get(
            "fuel_type",
            "",
        ),

        "vehicle_value": money(
            safe_float(
                seed.get("vehicle_value")
            )
        ),

        "seats": str(
            safe_int(seed.get("seats"))
        ),

        "power_to_weight_ratio": (
            f"{safe_float(seed.get('power_to_weight_ratio')):.2f}"
        ),

        "vehicle_brand": seed.get(
            "vehicle_brand",
            "",
        ),

        "municipality_type": seed.get(
            "municipality_type",
            "",
        ),

        "circulation_area": seed.get(
            "circulation_area",
            "",
        ),

        "total_premium": money(
            total_premium
        ),

        "liability_premium": money(
            safe_float(
                seed.get("liability_premium")
            ) * premium_multiplier
        ),

        "property_damage_premium": money(
            safe_float(
                seed.get(
                    "property_damage_premium"
                )
            ) * premium_multiplier
        ),

        "theft_premium": money(
            safe_float(
                seed.get("theft_premium")
            ) * premium_multiplier
        ),

        "fire_premium": money(
            safe_float(
                seed.get("fire_premium")
            ) * premium_multiplier
        ),

        "glass_premium": money(
            safe_float(
                seed.get("glass_premium")
            ) * premium_multiplier
        ),

        "legal_protection_premium": money(
            safe_float(
                seed.get(
                    "legal_protection_premium"
                )
            ) * premium_multiplier
        ),

        "occupants_premium": money(
            safe_float(
                seed.get("occupants_premium")
            ) * premium_multiplier
        ),

        "total_exposure": (
            f"{max(safe_float(seed.get('total_exposure')), 0.0):.6f}"
        ),

        "liability_exposure": (
            f"{max(safe_float(seed.get('liability_exposure')), 0.0):.6f}"
        ),

        "policy_start_date": (
            policy_start_date.isoformat()
        ),

        "policy_end_date": (
            policy_end_date.isoformat()
        ),

        "event_type": "POLICY_ISSUED",

        "event_timestamp": timestamp_string(
            event_timestamp
        ),
    }


# =========================================================
# CLAIM GENERATOR
# =========================================================

def generate_claim_event(
    seed: Dict[str, str],
    policy: Dict[str, str],
    event_timestamp: datetime,
) -> Dict[str, str]:

    total_claims = safe_int(
        seed.get("total_claims")
    )

    total_incurred = safe_float(
        seed.get("total_incurred")
    )

    # Use the reference dataset's claim statistics
    # as the basis for synthetic claim generation.
    if total_claims <= 0:
        total_claims = 1

    average_incurred = (
        total_incurred / total_claims
        if total_incurred > 0
        else random.uniform(
            500.0,
            5000.0,
        )
    )

    claimed_amount = (
        average_incurred
        * random.uniform(0.80, 1.20)
    )

    claim_types = [
        (
            "LIABILITY",
            "liability_claims",
        ),
        (
            "PROPERTY",
            "property_claims",
        ),
        (
            "THEFT",
            "theft_claims",
        ),
        (
            "FIRE",
            "fire_claims",
        ),
        (
            "GLASS",
            "glass_claims",
        ),
        (
            "LEGAL_PROTECTION",
            "legal_protection_claims",
        ),
        (
            "OCCUPANTS",
            "occupants_claims",
        ),
    ]

    eligible_types = [
        claim_type
        for claim_type, source_column in claim_types
        if safe_int(
            seed.get(source_column)
        ) > 0
    ]

    claim_type = (
        random.choice(eligible_types)
        if eligible_types
        else "PROPERTY"
    )

    claim_date = event_timestamp.date()

    incident_date = (
        claim_date
        - timedelta(
            days=random.randint(0, 30)
        )
    )

    return {
        "claim_id": new_id("CLM"),

        "claim_number": (
            f"CLM-{event_timestamp:%Y%m%d}-"
            f"{uuid.uuid4().hex[:10].upper()}"
        ),

        "policy_id": policy["policy_id"],

        "customer_id": policy["customer_id"],

        "insured_id": policy["insured_id"],

        "claim_type": claim_type,

        "claim_status": "REGISTERED",

        "claim_date": (
            claim_date.isoformat()
        ),

        "incident_date": (
            incident_date.isoformat()
        ),

        "settlement_date": "",

        "claimed_amount": money(
            claimed_amount
        ),

        "approved_amount": "0.00",

        "settled_amount": "0.00",

        "event_type": "CLAIM_REGISTERED",

        "event_timestamp": timestamp_string(
            event_timestamp
        ),
    }


# =========================================================
# WRITE PLAIN CSV
# =========================================================

def write_csv(
    rows: List[Dict[str, str]],
    columns: List[str],
    output_file: str,
) -> None:

    os.makedirs(
        os.path.dirname(output_file),
        exist_ok=True,
    )

    with open(
        output_file,
        "w",
        encoding="utf-8",
        newline="",
    ) as csvfile:

        writer = csv.DictWriter(
            csvfile,
            fieldnames=columns,
            extrasaction="ignore",
        )

        writer.writeheader()

        writer.writerows(rows)


# =========================================================
# MINIO UPLOAD
# =========================================================

def upload_file(
    hook: S3Hook,
    local_file: str,
    object_name: str,
) -> None:

    hook.load_file(
        filename=local_file,
        key=object_name,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )

    print(
        f"Uploaded: "
        f"s3://{MINIO_BUCKET}/{object_name}"
    )


# =========================================================
# GENERATE ONE 5-MINUTE BATCH
# =========================================================

def generate_batch(
    hook: S3Hook,
    reference_rows: List[Dict[str, str]],
    batch_timestamp: datetime,
    policy_count: int,
    claim_count: int,
    output_dir: str,
) -> Tuple[str, str]:

    date_path = batch_timestamp.strftime(
        "%Y/%m/%d"
    )

    timestamp = batch_timestamp.strftime(
        "%Y%m%d%H%M%S"
    )

    policy_file_name = (
        f"data_POLICY_{timestamp}.csv"
    )

    claim_file_name = (
        f"data_CLAIM_{timestamp}.csv"
    )

    policy_local_file = os.path.join(
        output_dir,
        policy_file_name,
    )

    claim_local_file = os.path.join(
        output_dir,
        claim_file_name,
    )

    # =====================================================
    # GENERATE POLICY EVENTS
    # =====================================================

    policies = []

    for _ in range(policy_count):

        seed = random.choice(
            reference_rows
        )

        policy = generate_policy_event(
            seed=seed,
            event_timestamp=batch_timestamp,
        )

        policies.append(policy)

    write_csv(
        rows=policies,
        columns=POLICY_COLUMNS,
        output_file=policy_local_file,
    )

    policy_object = (
        f"{POLICY_FOLDER}/"
        f"{date_path}/"
        f"{policy_file_name}"
    )

    upload_file(
        hook=hook,
        local_file=policy_local_file,
        object_name=policy_object,
    )

    # =====================================================
    # GENERATE CLAIM EVENTS
    # =====================================================

    claims = []

    for _ in range(claim_count):

        if not policies:
            break

        policy = random.choice(
            policies
        )

        seed = random.choice(
            reference_rows
        )

        claim = generate_claim_event(
            seed=seed,
            policy=policy,
            event_timestamp=batch_timestamp,
        )

        claims.append(claim)

    write_csv(
        rows=claims,
        columns=CLAIM_COLUMNS,
        output_file=claim_local_file,
    )

    claim_object = (
        f"{CLAIM_FOLDER}/"
        f"{date_path}/"
        f"{claim_file_name}"
    )

    upload_file(
        hook=hook,
        local_file=claim_local_file,
        object_name=claim_object,
    )

    # =====================================================
    # CLEANUP TEMPORARY FILES
    # =====================================================

    for local_file in (
        policy_local_file,
        claim_local_file,
    ):

        try:
            os.remove(local_file)

        except FileNotFoundError:
            pass

    return (
        policy_object,
        claim_object,
    )


# =========================================================
# MAIN GENERATOR ENTRYPOINT
# =========================================================

def generate_data_feed(
    files_per_run: int = FILES_PER_RUN,
    policy_records_per_file: int = (
        POLICY_RECORDS_PER_FILE
    ),
    claim_records_per_file: int = (
        CLAIM_RECORDS_PER_FILE
    ),
    output_dir: str = (
        "/tmp/data_source_generator"
    ),
) -> None:

    print("\n===================================")
    print("GENERATING FEEDS")
    print("===================================\n")

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    reference_rows = load_reference_data(
        hook=hook,
        sample_size=REFERENCE_SAMPLE_SIZE,
    )

    os.makedirs(
        output_dir,
        exist_ok=True,
    )

    start_time = datetime.now()

    total_files = 0

    for batch_number in range(
        files_per_run
    ):

        batch_timestamp = (
            start_time
            + timedelta(
                minutes=5 * batch_number
            )
        )

        print(
            f"\nGenerating batch "
            f"{batch_number + 1}/"
            f"{files_per_run}"
        )

        print(
            f"Batch timestamp: "
            f"{batch_timestamp}"
        )

        policy_object, claim_object = (
            generate_batch(
                hook=hook,
                reference_rows=reference_rows,
                batch_timestamp=batch_timestamp,
                policy_count=(
                    policy_records_per_file
                ),
                claim_count=(
                    claim_records_per_file
                ),
                output_dir=output_dir,
            )
        )

        print(
            f"Policy feed: {policy_object}"
        )

        print(
            f"Claim feed: {claim_object}"
        )

        total_files += 2

    print("\n===================================")
    print(
        f"TOTAL FILES GENERATED: "
        f"{total_files}"
    )
    print(
        "DATA FEED GENERATION "
        "COMPLETED"
    )
    print("===================================\n")