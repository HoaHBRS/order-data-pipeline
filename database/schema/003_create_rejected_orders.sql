IF OBJECT_ID(N'dbo.rejected_orders', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.rejected_orders
    (
        rejection_id  bigint IDENTITY(1,1) NOT NULL,
        order_id      nvarchar(100)  NULL,
        error_message nvarchar(1000) NOT NULL,
        raw_row       nvarchar(MAX)  NOT NULL,
        source_file   nvarchar(260)  NULL,
        rejected_at   datetime2(7)   NOT NULL
            CONSTRAINT DF_rejected_orders_rejected_at
            DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_rejected_orders PRIMARY KEY (rejection_id)
    );
END;
GO