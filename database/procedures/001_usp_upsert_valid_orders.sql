CREATE OR ALTER PROCEDURE dbo.usp_upsert_valid_orders
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH valid_ranked AS (
            SELECT
                LTRIM(RTRIM(order_id)) AS order_id,
                LTRIM(RTRIM(customer_id)) AS customer_id,
                UPPER(LTRIM(RTRIM(status))) AS status,
                TRY_CONVERT(INT, amount_cents) AS amount_cents,
                TRY_CONVERT(DATETIME2, updated_at, 126) AS updated_at,
                source_file,
                loaded_at,
                ROW_NUMBER() OVER (
                    PARTITION BY LTRIM(RTRIM(order_id))
                    ORDER BY
                        TRY_CONVERT(DATETIME2, updated_at, 126) DESC,
                        loaded_at DESC,
                        source_file DESC
                ) AS row_num
            FROM dbo.staging_orders
            WHERE NULLIF(LTRIM(RTRIM(order_id)), '') IS NOT NULL
              AND NULLIF(LTRIM(RTRIM(customer_id)), '') IS NOT NULL
              AND UPPER(LTRIM(RTRIM(status)))
                    IN ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED')
              AND TRY_CONVERT(INT, amount_cents) >= 0
              AND TRY_CONVERT(DATETIME2, updated_at, 126) IS NOT NULL
        )
        SELECT
            order_id,
            customer_id,
            status,
            amount_cents,
            updated_at,
            source_file,
            loaded_at
        INTO #valid_orders
        FROM valid_ranked
        WHERE row_num = 1;

        UPDATE target
        SET
            target.customer_id = source.customer_id,
            target.status = source.status,
            target.amount_cents = source.amount_cents,
            target.updated_at = source.updated_at,
            target.source_file = source.source_file,
            target.loaded_at = source.loaded_at
        FROM dbo.orders AS target
        INNER JOIN #valid_orders AS source
            ON target.order_id = source.order_id
        WHERE source.updated_at >= target.updated_at;

        INSERT INTO dbo.orders (
            order_id,
            customer_id,
            status,
            amount_cents,
            updated_at,
            source_file,
            loaded_at
        )
        SELECT
            source.order_id,
            source.customer_id,
            source.status,
            source.amount_cents,
            source.updated_at,
            source.source_file,
            source.loaded_at
        FROM #valid_orders AS source
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.orders AS target
            WHERE target.order_id = source.order_id
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