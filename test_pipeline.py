import sqlite3

from pipeline import (
    CSV_PATH,
    TransientPipelineError,
    extract_orders,
    filter_replay_rows,
    initialize_database,
    run_order_pipeline,
    run_order_pipeline_with_retry,
    run_order_replay,
    validate_replay_window,
)


REPLAY_START = "2026-08-01T08:15:00"
REPLAY_END = "2026-08-01T08:25:00"


def create_test_connection():
    connection = sqlite3.connect(":memory:")
    initialize_database(connection)
    return connection


def raise_simulated_replay_failure():
    raise RuntimeError("Simulated replay failure")


def test_incremental_idempotency():
    connection = create_test_connection()

    try:
        first_summary = run_order_pipeline(connection, CSV_PATH)
        second_summary = run_order_pipeline(connection, CSV_PATH)

        assert first_summary == {
            "extracted_count": 8,
            "valid_count": 4,
            "rejected_count": 4,
            "changed_count": 4,
            "quarantined_count": 4,
        }
        assert second_summary == {
            "extracted_count": 8,
            "valid_count": 4,
            "rejected_count": 4,
            "changed_count": 0,
            "quarantined_count": 0,
        }

        orders_count = connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0]
        rejected_count = connection.execute(
            "SELECT COUNT(*) FROM rejected_orders"
        ).fetchone()[0]
        watermark = connection.execute(
            """
            SELECT last_watermark
            FROM pipeline_state
            WHERE pipeline_name = 'orders_pipeline'
            """
        ).fetchone()[0]

        assert orders_count == 4
        assert rejected_count == 4
        assert watermark == "2026-08-01T08:35:00"

    finally:
        connection.close()

    print("test_incremental_idempotency: PASSED")


def test_successful_replay():
    connection = create_test_connection()

    try:
        rows = extract_orders(CSV_PATH)
        summary = run_order_replay(
            connection,
            rows,
            REPLAY_START,
            REPLAY_END,
        )

        assert summary == {
            "replay_count": 5,
            "valid_count": 3,
            "invalid_count": 2,
            "changed_count": 3,
            "quarantined_count": 2,
        }

        orders_count = connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0]
        rejected_count = connection.execute(
            "SELECT COUNT(*) FROM rejected_orders"
        ).fetchone()[0]

        assert orders_count == 3
        assert rejected_count == 2

    finally:
        connection.close()

    print("test_successful_replay: PASSED")


def test_replay_window_boundaries():
    rows = [
        {
            "order_id": "START",
            "updated_at": REPLAY_START,
        },
        {
            "order_id": "MIDDLE",
            "updated_at": "2026-08-01T08:20:00",
        },
        {
            "order_id": "END",
            "updated_at": REPLAY_END,
        },
    ]

    replay_rows = filter_replay_rows(
        rows,
        REPLAY_START,
        REPLAY_END,
    )
    selected_ids = [
        row["order_id"] for row in replay_rows
    ]

    assert selected_ids == ["START", "MIDDLE"]
    print("test_replay_window_boundaries: PASSED")


def test_invalid_replay_window():
    error_raised = False

    try:
        validate_replay_window(
            REPLAY_END,
            REPLAY_START,
        )
    except ValueError as error:
        error_raised = True
        assert str(error) == (
            "start_at must be earlier than end_at"
        )

    assert error_raised is True
    print("test_invalid_replay_window: PASSED")


def test_replay_idempotency():
    connection = create_test_connection()

    try:
        rows = extract_orders(CSV_PATH)

        first_summary = run_order_replay(
            connection,
            rows,
            REPLAY_START,
            REPLAY_END,
        )
        orders_after_first = connection.execute(
            "SELECT * FROM orders ORDER BY order_id"
        ).fetchall()
        rejected_after_first = connection.execute(
            """
            SELECT *
            FROM rejected_orders
            ORDER BY order_id
            """
        ).fetchall()

        second_summary = run_order_replay(
            connection,
            rows,
            REPLAY_START,
            REPLAY_END,
        )
        orders_after_second = connection.execute(
            "SELECT * FROM orders ORDER BY order_id"
        ).fetchall()
        rejected_after_second = connection.execute(
            """
            SELECT *
            FROM rejected_orders
            ORDER BY order_id
            """
        ).fetchall()

        assert first_summary["changed_count"] == 3
        assert first_summary["quarantined_count"] == 2
        assert second_summary["changed_count"] == 0
        assert second_summary["quarantined_count"] == 0
        assert orders_after_second == orders_after_first
        assert rejected_after_second == rejected_after_first

    finally:
        connection.close()

    print("test_replay_idempotency: PASSED")


def test_replay_rollback():
    connection = create_test_connection()
    error_raised = False

    try:
        rows = extract_orders(CSV_PATH)

        try:
            run_order_replay(
                connection,
                rows,
                REPLAY_START,
                REPLAY_END,
                before_commit_hook=raise_simulated_replay_failure,
            )
        except RuntimeError as error:
            error_raised = True
            assert str(error) == "Simulated replay failure"

        assert error_raised is True

        orders_after_failure = connection.execute(
            "SELECT * FROM orders"
        ).fetchall()
        rejected_after_failure = connection.execute(
            "SELECT * FROM rejected_orders"
        ).fetchall()

        assert orders_after_failure == []
        assert rejected_after_failure == []

    finally:
        connection.close()

    print("test_replay_rollback: PASSED")


def test_safe_rerun_after_failure():
    connection = create_test_connection()

    try:
        rows = extract_orders(CSV_PATH)

        try:
            run_order_replay(
                connection,
                rows,
                REPLAY_START,
                REPLAY_END,
                before_commit_hook=raise_simulated_replay_failure,
            )
        except RuntimeError:
            pass

        summary = run_order_replay(
            connection,
            rows,
            REPLAY_START,
            REPLAY_END,
        )

        assert summary["changed_count"] == 3
        assert summary["quarantined_count"] == 2

        orders_count = connection.execute(
            "SELECT COUNT(*) FROM orders"
        ).fetchone()[0]
        rejected_count = connection.execute(
            "SELECT COUNT(*) FROM rejected_orders"
        ).fetchone()[0]
        audit_rows = connection.execute(
            """
            SELECT run_type, replay_from, replay_to, status
            FROM pipeline_runs
            ORDER BY run_id
            """
        ).fetchall()

        assert orders_count == 3
        assert rejected_count == 2
        assert audit_rows == [
            (
                "REPLAY",
                REPLAY_START,
                REPLAY_END,
                "FAILED",
            ),
            (
                "REPLAY",
                REPLAY_START,
                REPLAY_END,
                "SUCCESS",
            ),
        ]

    finally:
        connection.close()

    print("test_safe_rerun_after_failure: PASSED")


def test_retry_after_transient_failure():
    connection = create_test_connection()
    failure_state = {"call_count": 0}

    def fail_first_attempt():
        failure_state["call_count"] += 1

        if failure_state["call_count"] == 1:
            raise TransientPipelineError(
                "Simulated temporary failure"
            )

    try:
        summary = run_order_pipeline_with_retry(
            connection,
            CSV_PATH,
            max_attempts=2,
            base_delay_seconds=0,
            sleep_function=lambda seconds: None,
            before_commit_hook=fail_first_attempt,
        )

        audit_statuses = connection.execute(
            """
            SELECT status
            FROM pipeline_runs
            ORDER BY run_id
            """
        ).fetchall()

        assert failure_state["call_count"] == 2
        assert summary["changed_count"] == 4
        assert summary["quarantined_count"] == 4
        assert audit_statuses == [
            ("FAILED",),
            ("SUCCESS",),
        ]

    finally:
        connection.close()

    print("test_retry_after_transient_failure: PASSED")


def run_all_tests():
    test_incremental_idempotency()
    test_successful_replay()
    test_replay_window_boundaries()
    test_invalid_replay_window()
    test_replay_idempotency()
    test_replay_rollback()
    test_safe_rerun_after_failure()
    test_retry_after_transient_failure()


if __name__ == "__main__":
    run_all_tests()
