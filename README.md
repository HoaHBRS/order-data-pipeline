# Order Pipeline Mini Project

A small Python and SQLite data pipeline for processing order data safely and
repeatably.

## What It Demonstrates

- CSV extraction with schema validation
- Row-level validation and rejected-order quarantine
- Incremental processing with a persisted watermark
- Idempotent SQLite upserts
- Replay and backfill using a half-open time window
- Transaction rollback and safe reruns after failure
- Pipeline audit records for successful and failed runs
- Retry with exponential backoff for transient incremental failures
- Command-line interface and structured logging
- Automated tests using isolated in-memory databases

## Project Structure

```text
order_pipeline_project/
├── pipeline.py
├── test_pipeline.py
├── README.md
├── .gitignore
└── data/
    └── orders_2026-08-01.csv
```

The SQLite database is generated at `data/order_pipeline.db` when the pipeline
runs. Database files are intentionally excluded from Git.

## Quick Start

```bash
git clone https://github.com/HoaHBRS/order-data-pipeline.git
cd order-data-pipeline
python3 test_pipeline.py
```

## Run the Incremental Pipeline

```bash
python3 pipeline.py incremental
```

Run the same command again to verify idempotency: previously processed rows
must not be written again.

## Run a Replay

```bash
python3 pipeline.py replay \
  --start-at "2026-08-01T08:15:00" \
  --end-at "2026-08-01T08:25:00"
```

Replay uses a half-open interval:

```text
start_at <= updated_at < end_at
```

## Run the Tests

```bash
python3 test_pipeline.py
```

The eight tests cover incremental and replay idempotency, replay boundaries,
invalid windows, rollback, safe reruns, and transient-error retry.

## Requirements

Python 3 with SQLite support. No third-party packages are required.

## Project Status

Completed and verified with eight automated tests.
