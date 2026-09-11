CREATE OR ALTER PROCEDURE dbo.usp_insert_rejected_orders
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH classified AS (
            SELECT
                s.order_id,
                s.source_file,
                CONCAT_WS('; ',
                    CASE
                        WHEN NULLIF(LTRIM(RTRIM(s.order_id)), '') IS NULL
                        THEN 'order_id is missing'
                    END,
                    CASE
                        WHEN NULLIF(LTRIM(RTRIM(s.customer_id)), '') IS NULL
                        THEN 'customer_id is missing'
                    END,
                    CASE
                        WHEN NULLIF(LTRIM(RTRIM(s.status)), '') IS NULL
                          OR UPPER(LTRIM(RTRIM(s.status))) NOT IN
                             ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED')
                        THEN 'status is invalid'
                    END,
                    CASE
                        WHEN TRY_CONVERT(INT, s.amount_cents) IS NULL
                          OR TRY_CONVERT(INT, s.amount_cents) < 0
                        THEN 'amount_cents is invalid'
                    END,
                    CASE
                        WHEN TRY_CONVERT(DATETIME2, s.updated_at, 126) IS NULL
                        THEN 'updated_at is invalid'
                    END
                ) AS error_message,
                (
                    SELECT
                        s.order_id AS order_id,
                        s.customer_id AS customer_id,
                        s.status AS status,
                        s.amount_cents AS amount_cents,
                        s.updated_at AS updated_at
                    FOR JSON PATH,
                        WITHOUT_ARRAY_WRAPPER,
                        INCLUDE_NULL_VALUES
                ) AS raw_row
            FROM dbo.staging_orders AS s
        ),
        rejection_candidates AS (
            SELECT DISTINCT
                order_id,
                error_message,
                raw_row,
                source_file
            FROM classified
            WHERE error_message <> ''
        )
        INSERT INTO dbo.rejected_orders (
            order_id,
            error_message,
            raw_row,
            source_file,
            rejected_at
        )
        SELECT
            candidate.order_id,
            candidate.error_message,
            candidate.raw_row,
            candidate.source_file,
            SYSUTCDATETIME()
        FROM rejection_candidates AS candidate
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.rejected_orders AS existing
            WHERE ISNULL(existing.order_id, '') =
                  ISNULL(candidate.order_id, '')
              AND existing.error_message = candidate.error_message
              AND existing.raw_row = candidate.raw_row
              AND ISNULL(existing.source_file, '') =
                  ISNULL(candidate.source_file, '')
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO