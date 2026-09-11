IF OBJECT_ID(N'dbo.pipeline_runs', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.pipeline_runs
    (
        pipeline_run_id     nvarchar(100)  NOT NULL,
        pipeline_name       nvarchar(200)  NOT NULL,
        source_file_pattern nvarchar(260)  NULL,
        run_status          nvarchar(20)   NOT NULL,
        rows_read           bigint         NULL,
        rows_copied         bigint         NULL,
        error_message       nvarchar(2000) NULL,
        logged_at           datetime2(7)   NOT NULL
            CONSTRAINT DF_pipeline_runs_logged_at
            DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_pipeline_runs PRIMARY KEY (pipeline_run_id)
    );
END;
GO