# Actives Feed Ingestion Pipeline

Data Engineering Intern Assessment — File Ingestion Pipeline. One Airflow
DAG (`ganesh_actives_ingestion`) that validates the daily Actives SIM feed
arriving in MinIO and loads the good records into Doris.

Lives at `dags/ganesh/actives_ingestion/` on branch `ganesh-nemmaluri`,
packaged as a self-contained subpackage (matching the pattern already
used by `dags/others/feed_simulator/` in this repo) rather than a single
file, given this DAG's scope (8 file-level checks, row-level rules,
dedup, checksum-based idempotency tracking).

```
validate_and_reject  →  load_raw_table
```

## Architecture

```
                         AIRFLOW (orchestration only)
                                   |
                     actives_ingestion_dag.py
                                   |
                +------------------+------------------+
                |                                     |
        src/pipeline/                           sql/ddl, sql/queries
        file_validation_pipeline.py             (all SQL is version-
        raw_load_pipeline.py                    controlled, never
                |                                inlined as strings
        src/validators/*                        in Python)
        src/loaders/*
        src/tracking/file_tracker.py
                |
        src/clients/minio_client.py  ---->  MinIO (crushftp bucket)
        src/clients/doris_client.py  ---->  Doris (database_ganesh)
```

The DAG file contains **only** orchestration: building clients from Airflow
Connections and calling into `src/pipeline`. Every filename/CSV/SQL/
validation/dedup rule lives in `src/` and `sql/`, independently testable
without Airflow. Client modules use `from minio import Minio` and
`airflow.hooks.base.BaseHook`/`airflow.decorators`, matching the
convention already established across this repo's other DAGs
(`task1_etl_dag.py` through `task8_end_to_end_dag.py`) rather than
`boto3`/`airflow.sdk` — this was a deliberate switch partway through
development once those existing DAGs were discovered, since that
combination is proven working against this exact deployed Airflow
instance, not just locally installed.

## Directory structure (relative to `dags/ganesh/actives_ingestion/`)

```
actives_ingestion_dag.py               Thin DAG: two tasks, no business logic
src/config/                            Canonical schema, constants, settings
src/clients/                           minio / PyMySQL (Doris) wrappers
src/validators/                        The 8 file-level checks (Task 1)
src/tracking/                          file_processing_log reads/writes
src/loaders/                           Row rules, dedup, raw-table insert (Task 2)
src/pipeline/                          Orchestration loops for each task
src/models/                            Plain data containers
src/utils/                             Checksum, logging, key-building helpers
sql/ddl/                               CREATE TABLE statements
sql/queries/                           Parameterized SELECT/INSERT statements
tests/                                 68 tests: unit + pipeline integration
scripts/run_local.py                   Runs the pipeline outside Airflow, for local verification
```

## Source → target data flow

```
SOURCE                  VALIDATION              VALIDATED             TARGET
MinIO                   8 ordered checks         MinIO                Doris
crushftp/                (src/validators/)       mediation/            database_ganesh
mediation/landing/  --->                    ---> validated/      ---> .raw_actives_daily
                                                                        (grain: 1 row/iccid)
                              |                                        |
                              v                                        v
                         rejected/<reason>/                    raw_actives_daily_rejected
                                                                 (row-level rejects)

                                                                 file_processing_log
                                                                 (control/tracking table,
                                                                  drives idempotency)
```

## How to run

1. **Doris**: the DDL in `sql/ddl/` creates `raw_actives_daily`,
   `raw_actives_daily_rejected`, and `file_processing_log` in the target
   database (`CREATE TABLE IF NOT EXISTS`, run automatically by the
   pipeline on every DAG run — no manual step required, but you can also
   run them by hand via `render_sql()`/`DBeaver` if you want to inspect
   them first).
2. **Airflow Connections** (Admin → Connections in the UI): `minio_default`
   and `doris_default` — same connection ids already used by this repo's
   other DAGs (`task1_etl_dag.py` etc.), so if those are already
   registered, nothing new is needed here.
3. This whole subtree (`dags/ganesh/actives_ingestion/`) is what the DAG
   processor needs to pick up — it's self-contained (mirrors
   `dags/others/feed_simulator/`'s pattern: the DAG file adds its own
   directory to `sys.path` so `from src...` imports resolve without any
   extra configuration).
4. Trigger `ganesh_actives_ingestion` manually from the UI (no schedule
   is set — see Assumptions).
5. **Local development / tests**: no Airflow or live infra required.
   ```
   cd dags/ganesh/actives_ingestion
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements-dev.txt
   pytest -q
   ```
   `src/config/settings.py` and the client modules fall back to plain
   environment variables (`MINIO_HOST`, `MINIO_ACCESS_KEY`,
   `MINIO_SECRET_KEY`, `DORIS_HOST`, `DORIS_PORT`, `DORIS_USER`,
   `DORIS_PASSWORD`) when Airflow isn't importable, so the same code
   works for local scripts/tests and inside Airflow unmodified.
   `scripts/run_local.py` runs the full pipeline this way, against real
   MinIO/Doris, without needing an Airflow deployment.

## Validation rules (Task 1)

**Non-destructive routing (revised after live feedback):** `landing/` is
a shared pool that other candidates' pipelines in this environment also
read from concurrently. Files are **copied** to `validated/` or
`rejected/<reason>/` — the original in `landing/` is never deleted.
Idempotency is therefore based on whether a file of the same *name*
already exists under `validated/` or any `rejected/<reason>/` prefix
(checked once per run, before any file content is read), not on whether
the file is still present in `landing/`. Scope is also narrowed up
front: only filenames containing `"Active"` (covers both `Active` and
`Actives`) are looked at at all — anything else in the shared landing
zone belongs to another feed or candidate and is never read, copied, or
otherwise touched.

Run in this exact order; the file is rejected at the **first** failing
check and copied to `rejected/<reason>/`:

| # | Check | Reason |
|---|-------|--------|
| 1 | Filename matches the Actives pattern | `pattern_mismatch` |
| 2 | File size > 0 bytes | `zero_size` |
| 3 | Has ≥1 data row below the header | `header_only` |
| 4 | Readable text, not binary/corrupted | `not_text_format` |
| 5 | Header has the 46 expected columns, in order | `invalid_column_count` |
| 6 | Every data row's field count matches the header | `invalid_column_count` |
| 7 | Not an exact content duplicate of a previously-processed file | `duplicate` |
| 8 | Sequence number not repeated/out of order for its region | `sequence_invalid` |

Efficiency note: checks 1–6 need only the object's key/size and, from
check 3 onward, its bytes. Checks 7–8 are the first point a SHA-256
checksum is computed and the tracking table is consulted — so a file
that's obviously not an Actives file (wrong filename) never gets
downloaded at all. This matters because `mediation/landing/` is a shared
zone (see **Scan scope**, below) holding far more non-Actives files than
Actives ones.

## Loading rules (Task 2)

For every file in `validated/`:
1. Reject a row (to `raw_actives_daily_rejected`) if, in order: `iccid`
   is empty → `NULL_OR_EMPTY_ICCID`; else `customernumber` is empty →
   `NULL_OR_EMPTY_CUSTOMER_NUMBER`; else `companyid` is empty/zero →
   `NULL_OR_ZERO_COMPANY_ID`.
2. Deduplicate remaining rows on `iccid`, keeping the last occurrence.
3. Stamp `file_name` and `loaded_at` on every loaded row.
4. Insert into `raw_actives_daily`.

## Idempotency strategy

A single Doris control table, `file_processing_log`, keyed by
**`file_checksum`** (SHA-256 of the file's raw stored bytes — the `.gz`
bytes for compressed files, not decompressed content), drives all three
stateful checks:

- **Task 1 duplicate check**: any existing tracking row for this
  checksum ⇒ `duplicate`.
- **Task 1 sequence check**: `MAX(sequence_number)` per region, computed
  only over rows with `validation_status = 'VALIDATED'` (a rejected
  file must not block a later legitimate sequence number), seeded once
  per run into an in-memory dict that's also updated after each
  newly-validated file — this covers both cross-run and intra-run
  ordering with one mechanism.
- **Task 2 load idempotency**: `load_status` on the tracking row —
  `LOADED` is skipped; `PENDING`/`LOAD_FAILED` is (re)attempted.

**Why a single shared table instead of separate validation/load
tables**: a file has one lifecycle
(landing → validated/rejected → loaded/load_failed). Splitting it would
just force a join on checksum whenever Task 2 needs "was this file
validated and not yet loaded," duplicating file_name/region/sequence
across two tables for no benefit.

**Why the checksum table, not the checksum table *alone***: once a file
is moved out of `landing/`, a rerun's listing simply won't see it again
— that alone makes re-validation and re-loading of an *already-moved*
file impossible by construction, with no database lookup needed. The
`file_processing_log` table exists specifically for the cases that
*do* need history: detecting a **new** file (still sitting in
`landing/`) that's a byte-identical re-delivery of something already
processed, and establishing the per-region sequence baseline. That's
why files rejected by checks 1–6 (bad filename, wrong schema, etc.)
never get a tracking row at all — content identity doesn't matter for
files that were never going to be considered "the same Actives file"
again, and skipping the checksum computation for them is exactly what
keeps validation cheap for the ~2,400 non-Actives files in the shared
landing zone.

**Crash-safety ordering**: the tracking row is always written *before*
the corresponding MinIO move (Task 1) or before a file is marked
`LOADED` (Task 2), so a crash between the two leaves a resumable state.
Specifically for Task 1: if a crash happens after the tracking write but
before the move completes, the file is still sitting in `landing/` on
the next run. Re-validating it would recompute the same checksum and
find its own tracking row — which `_validate_one_file` in
`file_validation_pipeline.py` detects (by comparing the row's
`landing_key` to the current file's key) and treats as **resuming a
stalled move**, not a duplicate, re-attempting the move to the
already-recorded destination without rewriting tracking state.

## Duplicate-file strategy

Content-based: SHA-256 over the exact bytes stored in MinIO (so `.gz`
bytes for compressed files — two files with identical decompressed
content but different gzip encoding are **not** treated as duplicates).
Filename is never used for duplicate detection.

## Row-deduplication interpretation

The assessment says: *"If the same row pair appears more than once in
the data you are loading, keep only the latest row for that pair and
drop the rest,"* without defining "row pair" or "latest."

- **Pair = `iccid`.** This matches the raw table's own
  `UNIQUE KEY(iccid)`, so the application-level "keep latest" logic and
  Doris's storage-level replace-on-insert agree instead of conflicting.
- **"Latest" = last physical row order within the file.** There is no
  timestamp column in the source data to sort by, so the last occurrence
  in file order is used as a deterministic, explicit tie-break
  (`pandas.drop_duplicates(subset=["iccid"], keep="last")`).
- **Cross-file collisions** (the same iccid appearing in two different
  files loaded in the same run) are *not* resolved by application logic
  — they're resolved by Doris's own `UNIQUE KEY(iccid)` replace-on-insert
  at the storage layer. To make that deterministic, `validated/` is
  processed in sorted key order, so the same file always "wins" a given
  iccid collision on a rerun.

## Rejected-row table design

`raw_actives_daily_rejected` — append-only audit log (`DUPLICATE KEY`,
no natural unique key, since it's diagnostic rather than authoritative):

| Column | Purpose |
|---|---|
| `file_name`, `row_number` | locate the offending row in its source file |
| `iccid`, `customernumber`, `companyid` | the three fields the rules check |
| `reject_reason` | one of the three `NULL_OR_*` reasons |
| `raw_row_json` | the full 46-column row, for debugging |
| `rejected_at` | load timestamp |

Idempotency for this table rides on file-level idempotency (a `LOADED`
file is never reprocessed, so its rejects are never re-inserted). The
one accepted edge case: a partial failure mid-file-load followed by a
`LOAD_FAILED` → retry could duplicate a few audit rows for that file.
Accepted tradeoff — this table is diagnostic, not authoritative; the raw
table is authoritative and self-heals via `UNIQUE KEY(iccid)`.

## Table data dictionary

**`raw_actives_daily`** — grain: one row per unique ICCID. Key: `iccid`.
Populated columns beyond the schema: `file_name` (source file),
`loaded_at` (load timestamp). DDL, column names/types, and
`UNIQUE KEY(iccid)` are exactly as given by the assessment and are not
altered.

**`raw_actives_daily_rejected`** — grain: one row per rejected source
row (see table above).

**`file_processing_log`** — grain: one row per distinct file *content*
(keyed by `file_checksum`). Tracks a file's full lifecycle: which region/
sequence it belongs to, its validation outcome and destination, and its
load status. See **Idempotency strategy** above.

## Error handling

- **Expected data errors** (bad filename, malformed row, null required
  field, duplicate, invalid sequence) never raise — they're modeled as
  `ValidationOutcome.fail(reason, detail)` and handled as normal control
  flow, never retried.
- **Infrastructure errors** (MinIO/Doris unavailable, unexpected
  exceptions) are caught per-file in both pipeline loops, logged with
  full context (file, stage, error type/message), and the loop
  continues to the next file — one bad file or one infra hiccup never
  stops the batch. Airflow's task-level retries (2 retries, 5 min delay)
  handle the case where the *whole task* needs to be retried (e.g., a
  transient connection failure before the loop even starts).

## Logging

Every file decision is logged as a single structured line
(`FILE_VALIDATED` / `FILE_REJECTED` / `FILE_LOAD_RESULT` / `FILE_ERROR`)
with file name, stage, reason, and relevant counts, plus a run-level
summary block (`ACTIVES FILE VALIDATION SUMMARY` / `ACTIVES RAW LOAD
SUMMARY`) with totals and a breakdown of rejection counts by reason —
visible directly in the Airflow task logs.

## Testing

```
pip install -r requirements-dev.txt
pytest -q
```

68 tests, no live infrastructure required:
- **Unit tests** for every validator (pure functions, no mocking), the
  tracking layer, the loaders, and MinIO client helpers.
- **MinIO** is faked with a hand-rolled `FakeMinioClient` (`tests/conftest.py`)
  mimicking the subset of the real `minio` library's API this code calls
  — there's no moto-equivalent mocking library for `minio` the way there
  is for boto3/S3. **Doris** is faked with a hand-rolled in-memory
  connection that specifically emulates `UNIQUE KEY` replace-on-insert
  semantics — deliberately not sqlite, whose constraint/upsert behavior
  doesn't match Doris's model.
- **Pipeline integration tests** run the full Task 1 and Task 2 loops
  end-to-end against fixtures covering all 8 file-level rejection
  reasons, all three row-level rejection reasons, intra-file dedup,
  cross-run idempotency, and per-file isolation.

Fixtures live in `tests/fixtures/` (a valid baseline file plus
header-only / bad-header / bad-row-count variants; other rejection
reasons — pattern mismatch, zero size, duplicate, sequence-invalid — are
exercised by feeding the baseline through different filenames/paths
rather than needing their own fixture files).

## Live verification results

Beyond the 68-test suite (fakes/mocks), the pipeline was run for real
against the live MinIO bucket (`crushftp`) and live Doris (`database_ganesh`)
using `scripts/run_local.py`. This surfaced two genuine findings, both
already reflected in the code:

1. **Doris rejected the assessment's literal DDL column order.** Real
   Doris requires `UNIQUE KEY` column(s) to be a leading prefix of the
   physical column declaration order:
   ```
   errCode = 2, detailMessage = Key columns should be a ordered prefix
   of the schema. KeyColumns[0] (starts from zero) is iccid, but
   corresponding column is file_name in the previous columns declaration.
   ```
   Fixed by moving `iccid` to be the first declared column in
   `sql/ddl/001_create_raw_actives_daily.sql` — column names, types, and
   the `UNIQUE KEY(iccid)` itself are unchanged; only the physical
   declaration order differs, and `INSERT` statements name columns
   explicitly so this has no effect on how rows are written or read.

   This was confirmed to be a hard engine constraint, not a workaround
   that was missed: four variants were tested live against the real
   cluster before accepting the reorder as necessary —
   `enable_unique_key_merge_on_write = "true"` with the original column
   order (failed, same error), `= "false"` with the original order
   (failed, same error), and two attempts at decoupling the physical
   sort/key order via an `ORDER BY(iccid)` clause (both failed —
   `mismatched input 'ORDER'` — this Doris version's grammar doesn't
   support that clause at all). Doris's storage engine physically sorts
   `UNIQUE KEY`/`DUPLICATE KEY`/`AGGREGATE KEY` tables by their key
   columns, which is why those columns must be a contiguous leading
   prefix; there is no table property that relaxes it. All four test
   tables were dropped after the experiment, and the real database
   (`database_ganesh`) was confirmed to have no leftover test tables.
2. **Every one of the 102 real Active-named files in the live landing
   zone failed check 6.** Byte-level inspection (raw semicolon counts,
   not a parsing artifact) showed the real data itself is "ragged":
   in the sample checked, 4,407 of 5,000 rows had only 36 of the 46
   expected fields — trailing optional columns (roughly PlanName
   through EID) appear to be omitted entirely by the source system when
   blank, rather than left as empty `;` placeholders. Per explicit
   direction, check 6 stays exactly as specified (reject on any field-
   count mismatch) rather than padding short rows — this is a real
   characteristic of the live feed, not a bug, and is worth flagging to
   whoever owns the upstream export if files are expected to pass.

   This was traced to its root: `dags/others/feed_simulator/generators/active_generator.py`
   generates the daily Active files by copying random rows out of a
   checked-in seed file, `sample_feed_files/HOL_01_Active_000050315_20260519230017.csv`.
   That seed file itself already has 87 of 99 rows with only 36 of 46
   fields — so the raggedness isn't introduced by the generator, it's
   baked into the original (evidently real/anonymized) sample data
   whoever built the simulator sourced. This makes check 6's strict
   rejection look like intended behavior being correctly exercised,
   not an artificial edge case.
3. **Client library migration re-verified live.** After discovering this
   repo's established convention (`airflow.decorators` + `BaseHook` +
   the `minio` library, used consistently across `task1_etl_dag.py`
   through `task8_end_to_end_dag.py`), the client modules were rewritten
   from `airflow.sdk`/`boto3` to match, and re-run against real MinIO —
   a file validated and moved successfully using the new `minio`-based
   client, confirming the swap didn't change behavior.

With those things confirmed, a small, clearly-labeled synthetic file
(`ZZZ_99_Active_000000001_20260922150000.csv`, region `ZZZ` — obviously
not a real region) was uploaded to the real `landing/` and run through
the full pipeline end-to-end:
- Validated and moved to `validated/`.
- Loaded into the real `raw_actives_daily` (2 rows, confirmed by direct
  query: correct `iccid`, `companyid`, `customernumber`, `loaded_at`).
- Rerunning the full pipeline afterward touched nothing: Task 1 found 0
  files (already moved out of `landing/`), Task 2 found the file but
  logged `FILE_LOAD_SKIPPED reason=already_loaded` rather than
  reprocessing it — idempotency confirmed against real infrastructure,
  not just the fakes.

The bulk validation run (2,542 real files, plus a second ~1,300-file
batch that had landed by the time the client-library swap was
re-verified) moved the non-Actives files into `rejected/pattern_mismatch/`
within the shared `landing/` zone each time, per the explicit scan-scope
decision in **Assumptions** below — worth knowing if anyone else's
pipeline also reads from that same landing prefix. Both rounds'
synthetic `ZZZ` test files/rows (`ZZZ_99_...` and `ZZZ_98_...`) were
deleted from `validated/`, `raw_actives_daily`, and `file_processing_log`
after each verification, leaving no test data behind either time.

## Assumptions (things this document left ambiguous)

1. **Scan scope (revised)**: `mediation/landing/` in the real environment
   is a **shared landing zone** with 2,542+ files across 8+ feed types
   (only ~102 are genuine Actives files; the rest are Tolling,
   SimInventory, CDRTrafficDetail, Activations, BOSSMED
   PortInOut/PreActive, eSIMHUB, DailyRecon, Sponsor, Product,
   IMEIChange, BOSS) **and is actively read/written by other candidates'
   pipelines in this shared assessment environment concurrently, not
   just other simulated feed types.** An earlier version of this
   pipeline scanned and moved everything in `landing/` literally,
   including deleting matched-and-rejected files from the shared pool —
   this caused real disruption to other candidates' testing and was
   corrected based on direct feedback. The pipeline now (a) only ever
   looks at filenames containing `"Active"`, ignoring everything else in
   the shared zone entirely, and (b) copies rather than moves, so
   `landing/` is never depleted for anyone else. See **Non-destructive
   routing** under Validation rules above.
2. The filename regex's timestamp field (`[0-9]+`) does not actually
   enforce "14 digits" despite the prose description — the regex is used
   exactly as given, so a shorter/longer numeric timestamp still passes
   check 1. See `test_regex_does_not_enforce_exact_14_digit_timestamp`.
3. Check 3 ("has a data row") is required to run before check 4 ("is
   readable text"). For a `.gz` file, decompression must succeed before
   either check can run; if decompression itself fails, that's reported
   as `not_text_format` (check 4's reason) even though it's detected
   ahead of check 3 — the one place check order deviates from strict
   numbering, since a file that can't be decompressed can't be evaluated
   for row content at all.
4. Check 5 (header validation) uses a single reason
   (`invalid_column_count`) for both a wrong column *count* and a
   correctly-counted-but-wrong-order/wrong-name header, since the
   assessment provides only one reason name for this check.
5. "Already processed" for the duplicate check (#7) means *any* existing
   tracking row for that checksum, regardless of its outcome — a
   byte-identical re-upload of a previously-*rejected* file is still
   flagged `duplicate`, not re-validated from scratch.
6. The sequence baseline (#8) only counts previously-**validated** files
   per region; a rejected file's sequence number doesn't block a later
   legitimate one.
7. `companyid` "empty or zero" is interpreted as the trimmed value being
   exactly `""` or `"0"` — a value like `"00"` would not be caught.
8. Doris database name: the assessment's `your_database_name` placeholder
   resolves to the existing database **`database_ganesh`**, per direct
   confirmation (not a new database created by this pipeline).
9. No DAG schedule was specified by the assessment; `schedule=None`
   (manual/external trigger) is the default, trivially changed via the
   `@dag(...)` decorator.
10. `landing/`, `validated/`, and `rejected/` are prefixes within one
    bucket (`crushftp`), not separate buckets — confirmed against the
    real MinIO instance.

## Known limitations

- A partial Task-2 failure followed by a retry can duplicate a small
  number of rows in `raw_actives_daily_rejected` (diagnostic table only
  — see **Rejected-row table design**).
- `file_processing_log` row updates are read-modify-write (fetch full
  row, re-INSERT), relying on Doris's `UNIQUE KEY` replace-on-insert
  rather than a partial `UPDATE` — chosen for simplicity and to stay
  version-agnostic across Doris releases, but it does mean two
  concurrent writers to the *same* file's tracking row could race (not
  expected in practice: only one DAG run processes a given file, since
  Task 1 empties `landing/` of anything it touches).
- No live end-to-end run through a deployed Airflow scheduler was
  performed as part of this build (no DAG-folder deployment access to
  the shared Airflow instance) — the pipeline logic was instead verified
  by calling `src/pipeline` directly against real MinIO/Doris
  (`scripts/run_local.py`) and through the full local test suite. Once
  this subtree is on the real DAG processor's path, trigger
  `ganesh_actives_ingestion` manually from the UI to verify end-to-end
  through the actual scheduler too.

## Troubleshooting runbook

**DAG fails during `validate_and_reject`** → check that task's logs for
`FILE_ERROR` lines (infra issue: file/stage/error type/message) versus
`FILE_REJECTED` lines (expected data issue: file/reason/detail) — the
`ACTIVES FILE VALIDATION SUMMARY` block at the end gives a rejection
breakdown by reason.

**DAG fails during `load_raw_table`** → same pattern in that task's
logs (`FILE_ERROR` / `FILE_LOAD_RESULT`); the `ACTIVES RAW LOAD SUMMARY`
block gives rows read/loaded/rejected and a rejection breakdown by
reason, per run.

**Unexpected row counts in `raw_actives_daily`** → check
`row_count_total`, `row_count_loaded`, `row_count_rejected`, and
`row_count_duplicate_dropped` on the file's `file_processing_log` row
(`SELECT * FROM file_processing_log WHERE file_name = '...'`), and cross-
reference `raw_actives_daily_rejected` for the specific rows/reasons.

**A file seems "stuck" and never appears in `validated/` or
`rejected/`** → it likely threw an unexpected exception during Task 1
(infra error, not a normal check failure); it's intentionally left in
`landing/` to retry on the next run. Check task logs for a
`FILE_ERROR stage=file_validation` line for that file's key.
