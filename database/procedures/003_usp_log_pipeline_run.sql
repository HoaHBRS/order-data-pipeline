CREATE OR ALTER PROCEDURE dbo.usp_log_pipeline_run
    @pipeline_run_id     nvarchar(100),
    @pipeline_name       nvarchar(200),
    @source_file_pattern nvarchar(260)  = NULL,
    @run_status          nvarchar(20),
    @rows_read           bigint         = NULL,
    @rows_copied         bigint         = NULL,
    @error_message       nvarchar(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.pipeline_runs
    SET
        pipeline_name       = @pipeline_name,
        source_file_pattern = @source_file_pattern,
        run_status          = @run_status,
        rows_read           = @rows_read,
        rows_copied         = @rows_copied,
        error_message       = @error_message,
        logged_at           = SYSUTCDATETIME()
    WHERE pipeline_run_id = @pipeline_run_id;

    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO dbo.pipeline_runs (
            pipeline_run_id,
            pipeline_name,
            source_file_pattern,
            run_status,
            rows_read,
            rows_copied,
            error_message
        )
        VALUES (
            @pipeline_run_id,
            @pipeline_name,
            @source_file_pattern,
            @run_status,
            @rows_read,
            @rows_copied,
            @error_message
        );
    END;
END;
GO