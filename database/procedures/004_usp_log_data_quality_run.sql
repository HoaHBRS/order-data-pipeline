CREATE OR ALTER PROCEDURE dbo.usp_log_data_quality_run
    @pipeline_run_id     nvarchar(100),
    @source_file_pattern nvarchar(260) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @total_rows               bigint,
        @valid_rows               bigint,
        @rejected_rows            bigint,
        @missing_order_id_rows    bigint,
        @missing_customer_id_rows bigint,
        @invalid_status_rows      bigint,
        @invalid_amount_rows      bigint,
        @invalid_updated_at_rows  bigint,
        @duplicate_order_id_rows  bigint,
        @dq_status                nvarchar(20);

    SELECT
        @total_rows = COUNT_BIG(*),

        @missing_order_id_rows =
            COALESCE(SUM(CASE
                WHEN NULLIF(LTRIM(RTRIM(s.order_id)), '') IS NULL
                THEN CAST(1 AS bigint) ELSE CAST(0 AS bigint)
            END), 0),

        @missing_customer_id_rows =
            COALESCE(SUM(CASE
                WHEN NULLIF(LTRIM(RTRIM(s.customer_id)), '') IS NULL
                THEN CAST(1 AS bigint) ELSE CAST(0 AS bigint)
            END), 0),

        @invalid_status_rows =
            COALESCE(SUM(CASE
                WHEN NULLIF(LTRIM(RTRIM(s.status)), '') IS NULL
                  OR UPPER(LTRIM(RTRIM(s.status))) NOT IN
                     ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED')
                THEN CAST(1 AS bigint) ELSE CAST(0 AS bigint)
            END), 0),

        @invalid_amount_rows =
            COALESCE(SUM(CASE
                WHEN TRY_CONVERT(int, s.amount_cents) IS NULL
                  OR TRY_CONVERT(int, s.amount_cents) < 0
                THEN CAST(1 AS bigint) ELSE CAST(0 AS bigint)
            END), 0),

        @invalid_updated_at_rows =
            COALESCE(SUM(CASE
                WHEN TRY_CONVERT(datetime2, s.updated_at, 126) IS NULL
                THEN CAST(1 AS bigint) ELSE CAST(0 AS bigint)
            END), 0),

        @rejected_rows =
            COALESCE(SUM(CASE
                WHEN NULLIF(LTRIM(RTRIM(s.order_id)), '') IS NULL
                  OR NULLIF(LTRIM(RTRIM(s.customer_id)), '') IS NULL
                  OR NULLIF(LTRIM(RTRIM(s.status)), '') IS NULL
                  OR UPPER(LTRIM(RTRIM(s.status))) NOT IN
                     ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED')
                  OR TRY_CONVERT(int, s.amount_cents) IS NULL
                  OR TRY_CONVERT(int, s.amount_cents) < 0
                  OR TRY_CONVERT(datetime2, s.updated_at, 126) IS NULL
                THEN CAST(1 AS bigint) ELSE CAST(0 AS bigint)
            END), 0)
    FROM dbo.staging_orders AS s;

    SELECT
        @duplicate_order_id_rows =
            COALESCE(SUM(d.row_count - 1), 0)
    FROM
    (
        SELECT COUNT_BIG(*) AS row_count
        FROM dbo.staging_orders
        WHERE NULLIF(LTRIM(RTRIM(order_id)), '') IS NOT NULL
        GROUP BY LTRIM(RTRIM(order_id))
        HAVING COUNT_BIG(*) > 1
    ) AS d;

    SET @valid_rows = @total_rows - @rejected_rows;

    SET @dq_status =
        CASE
            WHEN @rejected_rows = 0
             AND @duplicate_order_id_rows = 0
            THEN 'PASS'
            ELSE 'WARN'
        END;

    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE dbo.data_quality_report
        SET
            source_file_pattern      = @source_file_pattern,
            total_rows               = @total_rows,
            valid_rows               = @valid_rows,
            rejected_rows            = @rejected_rows,
            missing_order_id_rows    = @missing_order_id_rows,
            missing_customer_id_rows = @missing_customer_id_rows,
            invalid_status_rows      = @invalid_status_rows,
            invalid_amount_rows      = @invalid_amount_rows,
            invalid_updated_at_rows  = @invalid_updated_at_rows,
            duplicate_order_id_rows  = @duplicate_order_id_rows,
            dq_status                = @dq_status,
            checked_at               = SYSUTCDATETIME()
        WHERE pipeline_run_id = @pipeline_run_id;

        IF @@ROWCOUNT = 0
        BEGIN
            INSERT INTO dbo.data_quality_report
            (
                pipeline_run_id,
                source_file_pattern,
                total_rows,
                valid_rows,
                rejected_rows,
                missing_order_id_rows,
                missing_customer_id_rows,
                invalid_status_rows,
                invalid_amount_rows,
                invalid_updated_at_rows,
                duplicate_order_id_rows,
                dq_status
            )
            VALUES
            (
                @pipeline_run_id,
                @source_file_pattern,
                @total_rows,
                @valid_rows,
                @rejected_rows,
                @missing_order_id_rows,
                @missing_customer_id_rows,
                @invalid_status_rows,
                @invalid_amount_rows,
                @invalid_updated_at_rows,
                @duplicate_order_id_rows,
                @dq_status
            );
        END;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO
