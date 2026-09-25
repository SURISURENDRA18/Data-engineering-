# Actives Feed Ingestion Pipeline

## Overview

This project implements an Airflow ingestion pipeline for the Actives feed.

The pipeline contains two stages:

1. `validate_and_reject`
2. `load_raw_table`

The overall flow is:

MinIO landing
    |
    v
validate_and_reject
    |
    +---- valid files ------> mediation/venky/validated/
    |
    +---- invalid files ---> mediation/venky/rejected/<reason>/
                                  |
                                  v
                           load_raw_table
                                  |
                                  v
                                Doris

## Task 1 - File Handler and Validator

The DAG scans the Actives files from the landing location.

Files are validated in the required order:

1. Filename pattern
2. Zero-size check
3. Header-only check
4. Text/corruption check
5. Header column validation
6. Data-row column count validation
7. Exact duplicate check
8. Sequence validation

Valid files are moved to:

`mediation/venky/validated/`

Rejected files are moved to:

`mediation/venky/rejected/<reason>/`

The rejection reasons are:

- `pattern_mismatch`
- `zero_size`
- `header_only`
- `not_text_format`
- `invalid_column_count`
- `duplicate`
- `sequence_invalid`

## Task 2 - Raw Table Loader

Validated files are read from:

`mediation/venky/validated/`

Valid rows are loaded into the Doris raw table:

`raw_actives_daily`

Rows failing required-field validation are written to:

`raw_actives_daily_rejected`

The loader records:

- source file name
- load timestamp

## Airflow Connections

The DAG uses the following Airflow Connection IDs:

- `actives_venky_minio`
- `actives_venky_doris`

Credentials are stored in Airflow Connections and are not hard-coded in the source code.

## MinIO Isolation

The validation output paths are scoped to the `venky` namespace:

- `mediation/venky/validated/`
- `mediation/venky/rejected/`

This avoids writing output into other candidates' folders.

## Idempotency

The pipeline is designed so that files already processed by the loader are not loaded again.

A control/registry table is used to track processed files and their checksums.

## Assumptions

- The MinIO bucket is `crushftp`.
- The Actives landing path is `mediation/landing/`.
- The personal validated path is `mediation/venky/validated/`.
- The personal rejected path is `mediation/venky/rejected/`.
- Doris is accessed through its MySQL-compatible interface.
- Credentials are provided through Airflow Connections.
- The supplied Doris DDL is used as the basis for the raw table.
- The exact interpretation of the "row pair" requirement for de-duplication will be documented based on the available sample data and schema.

## DAG

DAG ID:

`venky_actives_ingestion`

Tasks:

`validate_and_reject >> load_raw_table`

## Running

The DAG is deployed to the provided Airflow environment.

After deployment, trigger:

`venky_actives_ingestion`

from the Airflow UI.

The task flow should complete in the following order:

`validate_and_reject`

then:

`load_raw_table`