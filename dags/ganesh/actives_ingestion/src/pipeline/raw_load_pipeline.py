"""Task 2 orchestration: load every file in validated/ into the raw table,
with per-file isolation and checksum-based load idempotency.

validated/ is processed in sorted key order so that, if the same iccid
appears in two different files within one run, Doris's UNIQUE KEY(iccid)
replace-on-insert resolves the collision deterministically (last file by
sorted key wins).
"""

import posixpath
from typing import Dict, Optional

from src.clients.minio_client import list_objects, read_object_bytes
from src.config.actives_schema import LoadStatus
from src.config.settings import Settings
from src.loaders.raw_loader import StreamLoadFn, ensure_raw_tables_exist, load_file_to_raw_table
from src.tracking.file_tracker import get_load_status, record_load_result
from src.utils.checksum import compute_sha256
from src.utils.logging_utils import get_logger, log_file_error, log_load_result, log_pipeline_summary


def run_raw_load(s3_client, conn, settings: Settings, stream_load_fn: Optional[StreamLoadFn] = None) -> dict:
    logger = get_logger()
    ensure_raw_tables_exist(conn, settings)

    objects = list_objects(s3_client, settings.bucket, settings.validated_prefix)
    objects.sort(key=lambda o: o["key"])

    files_loaded = 0
    files_skipped = 0
    files_failed = 0
    total_rows_loaded = 0
    total_rows_rejected = 0
    rejection_counts: Dict[str, int] = {}

    for obj in objects:
        key = obj["key"]
        filename = posixpath.basename(key)
        checksum: Optional[str] = None
        try:
            raw_bytes = read_object_bytes(s3_client, settings.bucket, key)
            checksum = compute_sha256(raw_bytes)

            if get_load_status(conn, settings, checksum) == LoadStatus.LOADED:
                files_skipped += 1
                logger.info(f"FILE_LOAD_SKIPPED file={filename} reason=already_loaded")
                continue

            result = load_file_to_raw_table(conn, settings, raw_bytes, filename, stream_load_fn=stream_load_fn)
            record_load_result(
                conn,
                settings,
                checksum,
                LoadStatus.LOADED,
                row_count_loaded=result.loaded_rows,
                row_count_rejected=result.rejected_rows,
                row_count_duplicate_dropped=result.duplicate_rows_dropped,
            )
            log_load_result(logger, result)

            files_loaded += 1
            total_rows_loaded += result.loaded_rows
            total_rows_rejected += result.rejected_rows
            for reason, count in result.rejected_by_reason.items():
                rejection_counts[reason] = rejection_counts.get(reason, 0) + count

        except Exception as exc:  # per-file isolation: log, mark failed, continue
            log_file_error(logger, key, "raw_load", exc)
            files_failed += 1
            if checksum is not None:
                try:
                    record_load_result(
                        conn,
                        settings,
                        checksum,
                        LoadStatus.LOAD_FAILED,
                        row_count_loaded=0,
                        row_count_rejected=0,
                        row_count_duplicate_dropped=0,
                    )
                except Exception as tracking_exc:
                    log_file_error(logger, key, "raw_load_tracking_update", tracking_exc)
            continue

    summary = {
        "files_discovered": len(objects),
        "files_loaded": files_loaded,
        "files_skipped_already_loaded": files_skipped,
        "files_failed": files_failed,
        "total_rows_loaded": total_rows_loaded,
        "total_rows_rejected": total_rows_rejected,
        **{f"rejected_{reason}": count for reason, count in rejection_counts.items()},
    }
    log_pipeline_summary(logger, "ACTIVES RAW LOAD SUMMARY", summary)
    return summary
