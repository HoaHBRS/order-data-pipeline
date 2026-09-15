:On Error exit

:r ./database/schema/001_create_staging_orders.sql
:r ./database/schema/002_create_orders.sql
:r ./database/schema/003_create_rejected_orders.sql
:r ./database/schema/004_create_pipeline_runs.sql
:r ./database/schema/005_create_data_quality_report.sql

:r ./database/procedures/001_usp_upsert_valid_orders.sql
:r ./database/procedures/002_usp_insert_rejected_orders.sql
:r ./database/procedures/003_usp_log_pipeline_run.sql
:r ./database/procedures/004_usp_log_data_quality_run.sql
