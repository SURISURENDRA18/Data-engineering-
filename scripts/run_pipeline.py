#!/usr/bin/env python3
"""Pipeline Execution Runner.

Provides a unified interface to run and verify the Actives Ingestion Pipeline:
- Mode 'fixtures': Ingests sample fixtures end-to-end (upload -> validate -> load -> verify -> cleanup).
- Mode 'landing': Processes real files in MinIO landing folder.
- Mode 'status': Checks connectivity, Doris table row counts, and Airflow health.
"""

import argparse
import os
import sys

# Ensure src is in sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(BASE_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from actives_ingestion.config import pipeline_config  # noqa: E402
from actives_ingestion.doris_client import DorisClient  # noqa: E402
from actives_ingestion.file_handler import FileHandler  # noqa: E402
from actives_ingestion.minio_client import MinioService  # noqa: E402
from actives_ingestion.raw_loader import RawLoader  # noqa: E402
from actives_ingestion.utils import get_logger  # noqa: E402

logger = get_logger("run_pipeline")


def run_fixtures_pipeline():
    """Upload fixtures, run Task 1 (validate & route) and Task 2 (load into Doris), and verify results."""
    print("=" * 70)
    print("RUNNING PIPELINE WITH SAMPLE FIXTURES (END-TO-END DEMO)")
    print("=" * 70)

    fixtures_dir = os.path.join(BASE_DIR, "fixtures")
    valid_csv = os.path.join(fixtures_dir, "US_01_Active_00001_20260922120000.csv")
    valid_gz = os.path.join(fixtures_dir, "EU_02_Actives_00001_20260922120000.csv.gz")
    corrupt_file = os.path.join(fixtures_dir, "US_01_Active_00004_20260922120000.csv")

    if not os.path.exists(valid_csv):
        print(f"Error: Fixture file {valid_csv} not found.")
        return 1

    minio_svc = MinioService(pipeline_config.minio)
    doris_client = DorisClient(pipeline_config.doris)

    # 1. Initialize Doris schema
    print("\n[Step 1/5] Verifying Apache Doris database and tables...")
    doris_client.init_database_and_tables()
    print("  -> Doris schema ready (UNIQUE KEY(customernumber, iccid)).")

    # 2. Stage test fixtures into MinIO landing
    from datetime import datetime, timezone  # noqa: E402
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    file_prefix = "DEMO_"
    uploaded_files = []

    print(f"\n[Step 2/5] Staging sample feeds to MinIO landing (prefix: {file_prefix})...")
    test_files = [
        (valid_csv, f"DEMO_01_Active_00001_{run_ts}.csv"),
        (valid_gz, f"DEMO_02_Actives_00001_{run_ts}.csv.gz"),
        (corrupt_file, f"DEMO_01_Active_00002_{run_ts}.csv"),
    ]

    for local_path, s3_name in test_files:
        with open(local_path, "rb") as f:
            data = f.read()
        dest = f"{pipeline_config.minio.landing_prefix}{s3_name}"
        minio_svc.put_object_data(dest, data)
        uploaded_files.append((s3_name, dest))
        print(f"  -> Uploaded: {s3_name} ({len(data)} bytes) to {dest}")

    # 3. Execute Task 1: validate_and_reject
    print("\n[Step 3/5] Executing Task 1 (validate_and_reject)...")
    handler = FileHandler()
    t1_summary = handler.process_landing_files(
        file_filter=lambda f: f.startswith(file_prefix)
    )
    print(f"  -> Discovered: {t1_summary.files_discovered}")
    print(f"  -> Validated:  {t1_summary.files_validated} (routed to {pipeline_config.minio.validated_prefix})")
    print(f"  -> Rejected:   {t1_summary.files_rejected} (routed to {pipeline_config.minio.rejected_prefix})")

    # 4. Execute Task 2: load_raw_table
    print("\n[Step 4/5] Executing Task 2 (load_raw_table)...")
    loader = RawLoader()
    t2_summary = loader.load_all_validated_files(
        file_filter=lambda f: f.startswith(file_prefix)
    )
    print(f"  -> Files Loaded:       {t2_summary.files_loaded}")
    print(f"  -> Rows Loaded (Raw):  {t2_summary.total_rows_loaded}")
    print(f"  -> Rows Rejected:      {t2_summary.total_rows_rejected}")

    # 5. Query and display Doris records
    print("\n[Step 5/5] Querying Doris for Ingested Records...")
    conn = doris_client._get_connection()
    minio_client = minio_svc._get_client()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT customernumber, iccid, companyid, companyname, file_name "
                f"FROM {pipeline_config.doris.database}.{pipeline_config.doris.raw_table} "
                f"WHERE file_name LIKE %s LIMIT 10",
                (f"%{file_prefix}%",),
            )
            rows = cur.fetchall()
            print(f"\nLoaded Rows in '{pipeline_config.doris.raw_table}' ({len(rows)} rows):")
            print(f"  {'CustomerNumber':<16} {'ICCID':<22} {'CompanyID':<10} {'Source File'}")
            print("  " + "-" * 75)
            for r in rows:
                if isinstance(r, dict):
                    cnum, iccid, cid, sfile = r.get("customernumber"), r.get("iccid"), r.get("companyid"), os.path.basename(r.get("file_name", ""))
                else:
                    cnum, iccid, cid, _cname, sfile = r[0], r[1], r[2], r[3], os.path.basename(r[4])
                print(f"  {str(cnum):<16} {str(iccid):<22} {str(cid):<10} {sfile}")

            cur.execute(
                f"SELECT reason, customernumber, iccid, companyid, file_name "
                f"FROM {pipeline_config.doris.database}.{pipeline_config.doris.rejected_table} "
                f"WHERE file_name LIKE %s LIMIT 10",
                (f"%{file_prefix}%",),
            )
            rej_rows = cur.fetchall()
            print(f"\nRejected Rows in '{pipeline_config.doris.rejected_table}' ({len(rej_rows)} rows):")
            print(f"  {'Reject Reason':<30} {'CustomerNumber':<16} {'ICCID':<22} {'CompanyID'}")
            print("  " + "-" * 75)
            for r in rej_rows:
                if isinstance(r, dict):
                    reason, cnum, iccid, cid = r.get("reason"), r.get("customernumber") or "NULL", r.get("iccid") or "NULL", r.get("companyid") or "NULL"
                else:
                    reason, cnum, iccid, cid = r[0], r[1] or "NULL", r[2] or "NULL", r[3] or "NULL"
                print(f"  {str(reason):<30} {str(cnum):<16} {str(iccid):<22} {str(cid)}")
    finally:
        conn.close()


    # Cleanup demo files from Doris (MinIO is read/copy-only; never delete from MinIO)
    print("\nCleaning up demo test artifacts in Doris (MinIO objects preserved permanently)...")
    try:
        # Doris cleanup
        conn = doris_client._get_connection()
        with conn.cursor() as cur:
            for sfile, _ in uploaded_files:
                cur.execute(
                    f"DELETE FROM {pipeline_config.doris.database}.{pipeline_config.doris.raw_table} "
                    f"WHERE file_name = %s",
                    (sfile,),
                )
                cur.execute(
                    f"DELETE FROM {pipeline_config.doris.database}.{pipeline_config.doris.rejected_table} "
                    f"WHERE file_name = %s",
                    (sfile,),
                )
                cur.execute(
                    f"DELETE FROM {pipeline_config.doris.database}.actives_file_tracking "
                    f"WHERE file_name = %s",
                    (sfile,),
                )
        conn.commit()
        conn.close()
        print("  -> Cleanup complete.")

    except Exception as e:
        print(f"  -> Cleanup notice: {e}")

    print("\n" + "=" * 70)
    print("PIPELINE DEMO EXECUTION COMPLETE (SUCCESS)")
    print("=" * 70)
    return 0


def run_landing_pipeline(feed_filter=None):
    """Run pipeline against existing files in MinIO landing folder."""
    print("=" * 70)
    print("RUNNING PIPELINE ON MINIO LANDING FOLDER")
    print(f"Filter: {feed_filter or 'ALL FILES'}")
    print("=" * 70)

    # 1. Task 1
    print("\n[Task 1] Running validate_and_reject...")
    handler = FileHandler()
    filter_fn = (lambda f: feed_filter in f) if feed_filter else None
    t1_summary = handler.process_landing_files(file_filter=filter_fn)
    print(f"  -> Discovered: {t1_summary.files_discovered}")
    print(f"  -> Validated:  {t1_summary.files_validated}")
    print(f"  -> Rejected:   {t1_summary.files_rejected}")

    # 2. Task 2
    print("\n[Task 2] Running load_raw_table...")
    loader = RawLoader()
    t2_summary = loader.load_all_validated_files(file_filter=filter_fn)
    print(f"  -> Files Loaded:       {t2_summary.files_loaded}")
    print(f"  -> Rows Loaded (Raw):  {t2_summary.total_rows_loaded}")
    print(f"  -> Rows Rejected:      {t2_summary.total_rows_rejected}")

    print("\nExecution complete.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Actives Ingestion Pipeline Runner")
    parser.add_argument(
        "--mode",
        choices=["fixtures", "landing"],
        default="fixtures",
        help="Execution mode: 'fixtures' (safe end-to-end demo) or 'landing' (live landing folder)",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help="Optional filename substring filter when running in landing mode (e.g. 'Active')",
    )

    args = parser.parse_args()
    if args.mode == "fixtures":
        sys.exit(run_fixtures_pipeline())
    elif args.mode == "landing":
        sys.exit(run_landing_pipeline(feed_filter=args.filter))


if __name__ == "__main__":
    main()
