IF OBJECT_ID(N'dbo.staging_orders', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.staging_orders
    (
        order_id    nvarchar(100) NULL,
        customer_id nvarchar(100) NULL,
        status      nvarchar(50)  NULL,
        amount_cents nvarchar(50) NULL,
        updated_at  nvarchar(100) NULL,
        source_file nvarchar(260) NULL,
        loaded_at   datetime2(7)  NOT NULL
            CONSTRAINT DF_staging_orders_loaded_at
            DEFAULT SYSUTCDATETIME()
    );
END;
GO