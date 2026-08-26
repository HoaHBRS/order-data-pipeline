import argparse
import csv
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
import time
from io import StringIO
from blob_storage import (
    read_blob_text,
    upload_rows_as_csv,
)


logger = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
CSV_PATH = DATA_DIR / "orders_2026-08-01.csv"
DB_PATH = DATA_DIR / "order_pipeline.db"

PIPELINE_NAME = "orders_pipeline"
DEFAULT_MAX_ATTEMPTS = 3

EXPECTED_COLUMNS = (
    "order_id",
    "customer_id",
    "status",
    "amount_cents",
    "updated_at",
)

REJECTED_COLUMNS = EXPECTED_COLUMNS + ("error_message",)

ALLOWED_STATUSES = frozenset(
    {
        "PENDING",
        "PAID",
        "SHIPPED",
        "CANCELLED",
    }
)


class TransientPipelineError(RuntimeError):
    """An error for which retrying the pipeline may succeed."""


class RetryExhaustedError(RuntimeError):
    """Raised when all retry attempts have failed."""


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_arguments(arguments=None):
    parser = argparse.ArgumentParser(description="Order data pipeline")

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    incremental_parser = subparsers.add_parser(
        "incremental",
        help="Run incremental pipeline",
    )

    incremental_parser.add_argument(
        "--source",
        default=str(CSV_PATH),
        help="Local CSV path or Blob Storage URL",
    )

    replay_parser = subparsers.add_parser(
        "replay",
        help="Replay orders within a time window",
    )
    replay_parser.add_argument(
        "--start-at",
        required=True,
        help="Inclusive replay start time",
    )
    replay_parser.add_argument(
        "--end-at",
        required=True,
        help="Exclusive replay end time",
    )
    replay_parser.add_argument(
        "--source",
        default=str(CSV_PATH),
        help="Local CSV path or Blob Storage URL",
    )

    return parser.parse_args(arguments)


# Database schema


def create_database_connection(database_path):
    return sqlite3.connect(str(database_path))


def create_orders_table(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id       TEXT PRIMARY KEY,
            customer_id    TEXT NOT NULL,
            status         TEXT NOT NULL,
            amount_cents   INTEGER NOT NULL CHECK (amount_cents >= 0),
            updated_at     TEXT NOT NULL
        )
        """
    )


def create_rejected_orders_table(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS rejected_orders (
            rejection_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id       TEXT,
            error_message  TEXT NOT NULL,
            raw_row        TEXT NOT NULL,
            rejected_at    TEXT NOT NULL,
            UNIQUE (error_message, raw_row)
        )
        """
    )


def create_pipeline_runs_table(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            status              TEXT NOT NULL,
            extracted_count     INTEGER NOT NULL,
            valid_count         INTEGER NOT NULL,
            rejected_count      INTEGER NOT NULL,
            changed_count       INTEGER NOT NULL,
            quarantined_count   INTEGER NOT NULL,
            started_at          TEXT NOT NULL,
            finished_at         TEXT,
            error_message       TEXT,
            run_type            TEXT NOT NULL DEFAULT 'INCREMENTAL',
            replay_from         TEXT,
            replay_to           TEXT
        )
        """
    )


def create_pipeline_state(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_state (
            pipeline_name   TEXT PRIMARY KEY,
            last_watermark  TEXT
        )
        """
    )


def initialize_database(connection):
    with connection:
        create_orders_table(connection)
        create_rejected_orders_table(connection)
        create_pipeline_runs_table(connection)
        create_pipeline_state(connection)


# Pipeline state and audit records


def initialize_pipeline_state(connection, pipeline_name):
    with connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO pipeline_state (
                pipeline_name,
                last_watermark
            )
            VALUES (?, NULL)
            """,
            (pipeline_name,),
        )


def get_watermark(connection, pipeline_name):
    row = connection.execute(
        """
        SELECT last_watermark
        FROM pipeline_state
        WHERE pipeline_name = ?
        """,
        (pipeline_name,),
    ).fetchone()

    return None if row is None else row[0]


def filter_new_orders(valid_rows, last_watermark):
    if last_watermark is None:
        return valid_rows.copy()

    watermark_datetime = datetime.fromisoformat(last_watermark)
    return [
        row
        for row in valid_rows
        if row["updated_at"] > watermark_datetime
    ]


def update_watermark(connection, pipeline_name, new_rows):
    if not new_rows:
        return None

    new_watermark = max(
        row["updated_at"] for row in new_rows
    ).isoformat()

    cursor = connection.execute(
        """
        UPDATE pipeline_state
        SET last_watermark = ?
        WHERE pipeline_name = ?
        """,
        (new_watermark, pipeline_name),
    )

    if cursor.rowcount != 1:
        raise RuntimeError(
            f"Could not update watermark for pipeline: {pipeline_name}"
        )

    return new_watermark


def start_pipeline_run(
    connection,
    run_type="INCREMENTAL",
    replay_from=None,
    replay_to=None,
):
    with connection:
        cursor = connection.execute(
            """
            INSERT INTO pipeline_runs (
                status,
                extracted_count,
                valid_count,
                rejected_count,
                changed_count,
                quarantined_count,
                started_at,
                run_type,
                replay_from,
                replay_to
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "RUNNING",
                0,
                0,
                0,
                0,
                0,
                utc_now_iso(),
                run_type,
                replay_from,
                replay_to,
            ),
        )

    return cursor.lastrowid


def finish_successful_run(connection, run_id, summary):
    with connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_runs
            SET
                status = ?,
                extracted_count = ?,
                valid_count = ?,
                rejected_count = ?,
                changed_count = ?,
                quarantined_count = ?,
                finished_at = ?,
                error_message = ?
            WHERE run_id = ? AND status = 'RUNNING'
            """,
            (
                "SUCCESS",
                summary["extracted_count"],
                summary["valid_count"],
                summary["rejected_count"],
                summary["changed_count"],
                summary["quarantined_count"],
                utc_now_iso(),
                None,
                run_id,
            ),
        )

        if cursor.rowcount != 1:
            raise RuntimeError(
                f"Could not finish successful run: run_id={run_id}"
            )


def finish_failed_run(connection, run_id, summary, error):
    with connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_runs
            SET
                status = ?,
                extracted_count = ?,
                valid_count = ?,
                rejected_count = ?,
                changed_count = ?,
                quarantined_count = ?,
                finished_at = ?,
                error_message = ?
            WHERE run_id = ? AND status = 'RUNNING'
            """,
            (
                "FAILED",
                summary["extracted_count"],
                summary["valid_count"],
                summary["rejected_count"],
                summary["changed_count"],
                summary["quarantined_count"],
                utc_now_iso(),
                str(error),
                run_id,
            ),
        )

        if cursor.rowcount != 1:
            raise RuntimeError(
                f"Could not finish failed run: run_id={run_id}"
            )


def fail_interrupted_runs(connection):
    with connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_runs
            SET
                status = ?,
                finished_at = ?,
                error_message = ?
            WHERE status = 'RUNNING'
            """,
            (
                "FAILED",
                utc_now_iso(),
                "Pipeline interrupted before completion",
            ),
        )

    return cursor.rowcount


# Extraction and validation
def extract_orders(csv_path):
    source = str(csv_path)

    if source.startswith("blob://"):
        container_name, blob_name = source[7:].split("/", 1)
        csv_file = StringIO(
            read_blob_text(container_name, blob_name)
        )
    else:
        csv_file = open(
            csv_path, "r", encoding="utf-8", newline=""
        )

    with csv_file:
        reader = csv.DictReader(csv_file)
        actual_columns = tuple(reader.fieldnames or ())

        if actual_columns != EXPECTED_COLUMNS:
            raise ValueError(
                "Unexpected CSV columns: "
                f"expected={EXPECTED_COLUMNS}, actual={actual_columns}"
            )

        return list(reader)


def validate_required_fields(row):
    for field_name in EXPECTED_COLUMNS:
        value = row.get(field_name)

        if value is None or str(value).strip() == "":
            raise ValueError(
                f"{row.get('order_id')}: missing {field_name}"
            )

    return row


def validate_status(row):
    if row["status"] not in ALLOWED_STATUSES:
        raise ValueError(
            f"{row.get('order_id')}: invalid status {row.get('status')}"
        )

    return row


def validate_amount(row):
    try:
        amount = int(row["amount_cents"])
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{row.get('order_id')}: invalid amount"
        ) from error

    if amount < 0:
        raise ValueError(
            f"{row.get('order_id')}: negative amount {amount}"
        )

    row["amount_cents"] = amount
    return row


def validate_updated_at(row):
    try:
        updated_at = datetime.fromisoformat(row["updated_at"])
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{row.get('order_id')}: "
            f"invalid updated_at {row.get('updated_at')}"
        ) from error

    row["updated_at"] = updated_at
    return row


def validate_order(row):
    validated_row = row.copy()
    validated_row = validate_required_fields(validated_row)
    validated_row = validate_status(validated_row)
    validated_row = validate_amount(validated_row)
    validated_row = validate_updated_at(validated_row)
    return validated_row


def validate_orders(rows):
    valid_rows = []
    rejected_rows = []

    for row in rows:
        raw_row = row.copy()

        try:
            valid_rows.append(validate_order(row))
        except ValueError as error:
            rejected_rows.append(
                {
                    "order_id": row.get("order_id"),
                    "error_message": str(error),
                    "raw_row": raw_row,
                }
            )

    return valid_rows, rejected_rows


def check_row_counts(extracted_rows, valid_rows, rejected_rows):
    extracted_count = len(extracted_rows)
    valid_count = len(valid_rows)
    rejected_count = len(rejected_rows)

    if valid_count + rejected_count != extracted_count:
        raise RuntimeError(
            "Row count mismatch: "
            f"extracted={extracted_count}, "
            f"valid={valid_count}, "
            f"rejected={rejected_count}"
        )


def check_duplicate_order_ids(valid_rows):
    seen_ids = set()
    duplicated_ids = set()

    for row in valid_rows:
        order_id = row["order_id"]

        if order_id in seen_ids:
            duplicated_ids.add(order_id)
        else:
            seen_ids.add(order_id)

    if duplicated_ids:
        raise RuntimeError(
            f"Duplicate order IDs: {sorted(duplicated_ids)}"
        )


# Loading


def load_orders(connection, rows):
    changed_count = 0

    for row in rows:
        cursor = connection.execute(
            """
            INSERT INTO orders (
                order_id,
                customer_id,
                status,
                amount_cents,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?)

            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_id = excluded.customer_id,
                status = excluded.status,
                amount_cents = excluded.amount_cents,
                updated_at = excluded.updated_at
            WHERE excluded.updated_at > orders.updated_at
            """,
            (
                row["order_id"],
                row["customer_id"],
                row["status"],
                row["amount_cents"],
                row["updated_at"].isoformat(),
            ),
        )
        changed_count += cursor.rowcount

    return changed_count


def load_rejected_orders(connection, rejected_rows):
    quarantined_count = 0

    for row in rejected_rows:
        cursor = connection.execute(
            """
            INSERT INTO rejected_orders (
                order_id,
                error_message,
                raw_row,
                rejected_at
            )
            VALUES (?, ?, ?, ?)

            ON CONFLICT (error_message, raw_row)
            DO NOTHING
            """,
            (
                row["order_id"],
                row["error_message"],
                json.dumps(row["raw_row"], sort_keys=True),
                utc_now_iso(),
            ),
        )
        quarantined_count += cursor.rowcount

    return quarantined_count


# Incremental pipeline and retry


def empty_run_summary():
    return {
        "extracted_count": 0,
        "valid_count": 0,
        "rejected_count": 0,
        "changed_count": 0,
        "quarantined_count": 0,
    }


def run_order_pipeline(
    connection,
    csv_path,
    before_commit_hook=None,
):
    summary = empty_run_summary()

    fail_interrupted_runs(connection)
    initialize_pipeline_state(connection, PIPELINE_NAME)
    last_watermark = get_watermark(connection, PIPELINE_NAME)
    run_id = start_pipeline_run(connection)

    try:
        extracted_rows = extract_orders(csv_path)
        summary["extracted_count"] = len(extracted_rows)

        valid_rows, rejected_rows = validate_orders(extracted_rows)
        summary["valid_count"] = len(valid_rows)
        summary["rejected_count"] = len(rejected_rows)

        check_row_counts(extracted_rows, valid_rows, rejected_rows)
        check_duplicate_order_ids(valid_rows)

        new_rows = filter_new_orders(valid_rows, last_watermark)

        with connection:
            changed_count = load_orders(connection, new_rows)
            quarantined_count = load_rejected_orders(
                connection,
                rejected_rows,
            )
            update_watermark(connection, PIPELINE_NAME, new_rows)

            if before_commit_hook is not None:
                before_commit_hook()

        source = str(csv_path)

        if source.startswith("blob://"):
            source_blob_name = source[7:].split("/", 1)[1]

            upload_rows_as_csv(
                container_name="processed",
                blob_name=source_blob_name,
                rows=valid_rows,
                fieldnames=EXPECTED_COLUMNS,
            )

            rejected_output_rows = [
                {
                    **row["raw_row"],
                    "error_message": row["error_message"],
                }
                for row in rejected_rows
            ]

            upload_rows_as_csv(
                container_name="rejected",
                blob_name=source_blob_name,
                rows=rejected_output_rows,
                fieldnames=REJECTED_COLUMNS,
            )

        summary["changed_count"] = changed_count
        summary["quarantined_count"] = quarantined_count
        finish_successful_run(connection, run_id, summary)

    except Exception as error:
        finish_failed_run(connection, run_id, summary, error)
        raise

    return summary


def is_retryable_error(error):
    return isinstance(error, TransientPipelineError)


def classify_sqlite_error(error):
    if not isinstance(error, sqlite3.OperationalError):
        return error

    message = str(error).lower()

    if "locked" in message or "busy" in message:
        return TransientPipelineError(str(error))

    return error


def should_retry(error, attempt_number, max_attempts):
    return (
        is_retryable_error(error)
        and attempt_number < max_attempts
    )


def run_order_pipeline_with_retry(
    connection,
    csv_path,
    max_attempts=DEFAULT_MAX_ATTEMPTS,
    base_delay_seconds=1,
    sleep_function=time.sleep,
    before_commit_hook=None,
):
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt_number in range(1, max_attempts + 1):
        try:
            return run_order_pipeline(
                connection,
                csv_path,
                before_commit_hook=before_commit_hook,
            )

        except Exception as error:
            classified_error = classify_sqlite_error(error)

            if should_retry(
                classified_error,
                attempt_number,
                max_attempts,
            ):
                delay_seconds = (
                    base_delay_seconds
                    * (2 ** (attempt_number - 1))
                )
                logger.info(
                    "Retrying incremental pipeline: "
                    "attempt=%s/%s delay_seconds=%s error=%s",
                    attempt_number + 1,
                    max_attempts,
                    delay_seconds,
                    classified_error,
                )
                sleep_function(delay_seconds)
                continue

            if is_retryable_error(classified_error):
                raise RetryExhaustedError(
                    "Pipeline failed after "
                    f"{max_attempts} attempts"
                ) from error

            raise

    raise AssertionError("Retry loop ended unexpectedly")


# Replay and backfill


def validate_replay_window(start_at, end_at):
    start_time = datetime.fromisoformat(start_at)
    end_time = datetime.fromisoformat(end_at)

    if start_time >= end_time:
        raise ValueError("start_at must be earlier than end_at")

    return start_time, end_time


def filter_replay_rows(rows, start_at, end_at):
    start_time, end_time = validate_replay_window(
        start_at,
        end_at,
    )
    replay_rows = []

    for row in rows:
        try:
            updated_time = datetime.fromisoformat(row["updated_at"])
        except (TypeError, ValueError):
            # Invalid timestamps cannot be placed in a time window.
            # Include them so validation can quarantine them.
            replay_rows.append(row)
            continue

        if start_time <= updated_time < end_time:
            replay_rows.append(row)

    return replay_rows


def run_order_replay(
    connection,
    rows,
    start_at,
    end_at,
    before_commit_hook=None,
):
    run_id = start_pipeline_run(
        connection,
        "REPLAY",
        start_at,
        end_at,
    )
    summary = empty_run_summary()

    try:
        replay_rows = filter_replay_rows(
            rows,
            start_at,
            end_at,
        )
        valid_rows, rejected_rows = validate_orders(replay_rows)

        summary["extracted_count"] = len(replay_rows)
        summary["valid_count"] = len(valid_rows)
        summary["rejected_count"] = len(rejected_rows)

        check_row_counts(replay_rows, valid_rows, rejected_rows)
        check_duplicate_order_ids(valid_rows)

        with connection:
            changed_count = load_orders(connection, valid_rows)
            quarantined_count = load_rejected_orders(
                connection,
                rejected_rows,
            )

            if before_commit_hook is not None:
                before_commit_hook()

        summary["changed_count"] = changed_count
        summary["quarantined_count"] = quarantined_count
        finish_successful_run(connection, run_id, summary)

    except Exception as error:
        finish_failed_run(connection, run_id, summary, error)
        raise

    return {
        "replay_count": summary["extracted_count"],
        "valid_count": summary["valid_count"],
        "invalid_count": summary["rejected_count"],
        "changed_count": summary["changed_count"],
        "quarantined_count": summary["quarantined_count"],
    }


# Command-line entry point


def execute_command(connection, args):
    if args.command == "incremental":
        return run_order_pipeline_with_retry(
            connection,
            args.source,
        )

    if args.command == "replay":
        rows = extract_orders(args.source)
        return run_order_replay(
            connection,
            rows,
            args.start_at,
            args.end_at,
        )

    raise ValueError(f"Unsupported command: {args.command}")


def main(arguments=None):
    configure_logging()
    args = parse_arguments(arguments)

    logger.info(
        "Starting order pipeline: command=%s",
        args.command,
    )

    connection = None

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        connection = create_database_connection(DB_PATH)
        initialize_database(connection)

        summary = execute_command(connection, args)

        for key, value in summary.items():
            logger.info("%s=%s", key, value)

        logger.info(
            "Order pipeline completed successfully: command=%s",
            args.command,
        )

    except Exception:
        logger.exception(
            "Order pipeline failed: command=%s",
            args.command,
        )
        raise

    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
