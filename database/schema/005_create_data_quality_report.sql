IF OBJECT_ID(N'dbo.data_quality_report', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.data_quality_report
    (
        pipeline_run_id          nvarchar(100) NOT NULL,
        source_file_pattern      nvarchar(260) NULL,
        total_rows               bigint        NOT NULL,
        valid_rows               bigint        NOT NULL,
        rejected_rows            bigint        NOT NULL,
        missing_order_id_rows    bigint        NOT NULL,
        missing_customer_id_rows bigint        NOT NULL,
        invalid_status_rows      bigint        NOT NULL,
        invalid_amount_rows      bigint        NOT NULL,
        invalid_updated_at_rows  bigint        NOT NULL,
        duplicate_order_id_rows  bigint        NOT NULL,
        dq_status                nvarchar(20)  NOT NULL,
        checked_at               datetime2(0)  NOT NULL
            CONSTRAINT DF_data_quality_report_checked_at
            DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_data_quality_report
            PRIMARY KEY (pipeline_run_id)
    );
END;
GO