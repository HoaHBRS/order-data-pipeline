# Azure SQL database scripts

This directory contains the version-controlled Azure SQL objects used by
the order ingestion pipeline.

## Deployment order

1. Run the files in `schema/` in numeric order.
2. Run the files in `procedures/` in numeric order.

The schema scripts create tables only when they do not already exist.
The procedure scripts use `CREATE OR ALTER` so they can be rerun safely.

Credentials and connection strings must never be stored here.
Local SQL scratch files remain in `/sql` and are ignored by Git.