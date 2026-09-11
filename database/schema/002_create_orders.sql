IF OBJECT_ID(N'dbo.orders', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.orders
    (
        order_id     nvarchar(100) NOT NULL,
        customer_id  nvarchar(100) NOT NULL,
        status       nvarchar(50)  NOT NULL,
        amount_cents int           NOT NULL,
        updated_at   datetime2(7)  NOT NULL,
        source_file  nvarchar(260) NULL,
        loaded_at    datetime2(7)  NOT NULL
            CONSTRAINT DF_orders_loaded_at
            DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_orders PRIMARY KEY (order_id)
    );
END;
GO