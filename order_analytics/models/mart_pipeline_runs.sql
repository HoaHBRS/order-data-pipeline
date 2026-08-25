SELECT
    run_id,
    run_type,
    status,
    extracted_count,
    valid_count,
    rejected_count,
    changed_count,
    quarantined_count,
    started_at,
    finished_at,
    error_message
FROM {{ source('order_pipeline', 'pipeline_runs') }}
