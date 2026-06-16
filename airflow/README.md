# UrbanPulse Airflow

Airflow has been added to give the offline data and ML workflow one visible
entry point. It is not used for the realtime serving path. The live map still
runs through FastAPI, Redis, the realtime gateway, and the frontend.

The main DAG is `urbanpulse_ml_alerts`.

```text
optional CSV fetch
  -> operational DB migrations
  -> DuckDB and Parquet lake
  -> delay feature dataset
  -> delay-increase model training
  -> scored alert JSON
  -> pipeline quality validation
  -> artifact validation
```

## What Has Been Built

A small Airflow profile has been added to Docker Compose. It includes an Airflow
metadata database, an initialization container, a webserver, a scheduler, and a
custom image with the ML dependencies used by the DAG.

The repository is mounted into the Airflow containers at `/opt/urbanpulse`, so
the DAG can run the existing project scripts instead of maintaining a second
copy of the pipeline logic.

## How It Works Locally

Airflow writes logs and ML outputs through the mounted repository directory. A
host UID is therefore stored in `.env` so generated files are not owned by an
unexpected container user.

```bash
grep -q '^AIRFLOW_UID=' .env || echo "AIRFLOW_UID=$(id -u)" >> .env
mkdir -p airflow/logs
docker compose --profile airflow up airflow-init
docker compose --profile airflow up -d airflow-webserver airflow-scheduler
```

The web UI is exposed at `http://127.0.0.1:8080`. The local account is created
from `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` in `.env`.

The DAG is paused by default. The first run is expected to be started manually
from the UI so the task graph and logs can be inspected.

## Inputs And Outputs

The DAG can work from staged CSV chunks, or it can refresh those chunks from the
VPS before the lake is updated.

```text
ml/data/raw/vehicle_exports/vehicle_positions_*.csv.gz
```

When `URBANPULSE_FETCH_EXPORTS=true`, the first task runs
`ml/scripts/fetch_vehicle_exports.sh`. That script can first connect to the VPS,
run the remote `export_vehicle_positions.sh`, and then copy
`vehicle_positions_*.csv.gz` back into the ignored local raw-data staging
directory. The remote staging directory is cleaned before and after a successful
fetch, and the local archive bundle is disabled by default for Airflow runs.

The normal Airflow path is incremental. The fetch task reads the current lake
manifest, subtracts a small lookback from the latest ingested timestamp, and
exports only that window from the VPS. The window is capped so a stale local
lake does not trigger weeks of catch-up work on a small VPS. The lake task then
replaces only the affected `service_date=...` Parquet partitions. When no lake
manifest exists, a small recent bootstrap window is exported.

The Airflow containers mount the host SSH directory at `/home/airflow/.ssh` in
read-only mode. In local development, the host UID from `.env` is used so the
container can read the same SSH keys that are used from the terminal.

When fresh CSVs have been fetched, the lake is updated incrementally from those
exports so the rest of the DAG uses the new VPS data. When no fetch has been
requested, `ml/lake/vehicle_positions` already exists, and
`URBANPULSE_REBUILD_LAKE=false`, the lake task refreshes DuckDB views against the
existing Parquet files instead.

The main produced artifact is the scored delay-risk file.

```text
ml/models/delay_increase_alerts.json
```

That file is served by the FastAPI endpoint `GET /delay-increase-alerts` and is
used by the frontend risk panel.

A pipeline quality report is also written after scoring.

```text
ml/reports/data_quality_latest.json
```

That report records the lake row counts, feature row counts, model metadata,
feature schema checks, alert artifact freshness, warnings, and failures. The DAG
is failed when the report contains failed checks. A zero-alert run is treated as
a warning by default because quiet traffic can be real, while stale alert rows
and missing artifacts are treated as failures.

## Settings

Most runtime settings can be supplied through `.env` before container startup or
through lowercase Airflow Variables. The important ones are:

- `URBANPULSE_FETCH_EXPORTS=false`
- `URBANPULSE_REBUILD_LAKE=false`
- `URBANPULSE_LAKE_INPUT_GLOB=ml/data/raw/vehicle_exports/vehicle_positions_*.csv.gz`
- `URBANPULSE_EXPORT_DOWNLOAD_DIR=/opt/urbanpulse/ml/data/raw/vehicle_exports`
- `URBANPULSE_CREATE_EXPORT_ARCHIVE=false`
- `URBANPULSE_REMOTE_USER=deploy`
- `URBANPULSE_REMOTE_HOST=vps.example.com`
- `URBANPULSE_REMOTE_DIR=/tmp/urbanpulse-vehicle-exports`
- `URBANPULSE_REMOTE_REPO_DIR=/opt/urban-pulse`
- `URBANPULSE_REMOTE_EXPORT_BEFORE_FETCH=true`
- `URBANPULSE_REMOTE_EXPORT_SCRIPT=export_vehicle_positions.sh`
- `URBANPULSE_REMOTE_INCREMENTAL_EXPORT=true`
- `URBANPULSE_REMOTE_INCREMENTAL_LOOKBACK_HOURS=2`
- `URBANPULSE_REMOTE_INCREMENTAL_BOOTSTRAP_DAYS=3`
- `URBANPULSE_REMOTE_INCREMENTAL_MAX_DAYS=3`
- `URBANPULSE_REMOTE_EXPORT_DAYS=35`
- `URBANPULSE_REMOTE_CONTAINER_NAME=prague_db`
- `URBANPULSE_REMOTE_DB_USER=urban`
- `URBANPULSE_REMOTE_DB_NAME=prague_transport`
- `URBANPULSE_SSH_OPTS=-o StrictHostKeyChecking=accept-new`
- `URBANPULSE_FEATURE_LATEST_DATES=3`
- `URBANPULSE_TRAIN_MAX_ROWS=1000000`
- `URBANPULSE_TRAIN_ROW_CAP=300000`
- `URBANPULSE_HOLDOUT_LAST_FILES=1`
- `URBANPULSE_LEARNING_CURVE_ROWS=50000,150000,300000`
- `URBANPULSE_DUCKDB_MEMORY_LIMIT=8GB`
- `URBANPULSE_DUCKDB_THREADS=2`
- `URBANPULSE_QUALITY_REPORT=ml/reports/data_quality_latest.json`
- `URBANPULSE_REALTIME_EVAL_REPORT=ml/reports/realtime_delay_prediction_eval.json`
- `URBANPULSE_MIN_LAKE_ROWS=1000`
- `URBANPULSE_MIN_FEATURE_ROWS=1000`
- `URBANPULSE_MAX_ALERT_ROW_AGE_HOURS=48`
- `URBANPULSE_FAIL_ON_ZERO_ALERTS=false`
- `WANDB_PROJECT=urbanpulse`
- `WANDB_MODE=online`

For example, `URBANPULSE_FEATURE_LATEST_DATES` can also be represented as the
Airflow Variable `urbanpulse_feature_latest_dates`.

W&B logging is enabled when `WANDB_API_KEY` is present in the Airflow container
environment. The training and scoring tasks then publish metrics and artifacts
to the configured W&B project. The classifier logs one final score snapshot for
run-to-run comparison, and it also logs a learning-curve table and stepped
metrics so model progress can be inspected inside a single run.

## What Comes Next

The DAG is currently a local orchestration layer around scripts that already
exist in the repository. The first quality gate has been added around generated
artifacts and model outputs. The same shape is intended to grow toward object
storage, a remote metadata database, richer data-quality checks, and a managed
runtime when AWS deployment becomes the next serious step.
