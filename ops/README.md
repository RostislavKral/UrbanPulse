# UrbanPulse Ops

This folder contains small operational helpers for the VPS collector. The VPS is
treated as a live collector and short-term operational store, not as the final
home for analytical history.

Longer-term history is expected to move into compressed exports, a local
DuckDB/Parquet lake, and object storage once the project is ready for cloud
deployment.

## What Has Been Built

- `vps_audit.sh` collects a read-only inventory of disk usage, Docker state,
  Compose services, database size, and collected date ranges.
- `vps_cleanup_logs.sh` configures Docker JSON log rotation and truncates
  oversized current logs.
- `vps_backup_schema.sh` creates compressed schema and role backups without
  dumping all table data.
- `vps_export_vehicle_positions_csv.sh` wraps the existing remote CSV export
  flow for vehicle-position history.

The helpers intentionally do not default to a concrete host. Connection details
are supplied through environment variables so personal infrastructure is not
encoded in the repository.

```bash
VPS_HOST=example.com VPS_USER=deploy ./ops/vps_audit.sh
```

## How The VPS Is Being Treated

The current TimescaleDB image uses `PGDATA=/home/postgres/pgdata/data`. An older
Compose configuration mounted the named volume at `/var/lib/postgresql/data`,
which meant live database files were placed in the container writable layer.

That state has been left intentionally cautious. The live database container is
not meant to be recreated until data has been exported or migrated.

The safer migration shape has been documented as:

1. Historical data is exported or backed up to external storage.
2. Ingestion is stopped.
3. A correctly mounted TimescaleDB container is created.
4. Data is restored or imported.
5. Ingestion is started again.

DuckDB and Parquet work is expected to build on exported historical data rather
than querying the live operational database directly.

## What Comes Next

The VPS is expected to become more boring over time. Log rotation, backup
checks, export automation, and retention rules are being made predictable before
more services are added there. Terraform and AWS are expected to take over the
durable infrastructure story later.
