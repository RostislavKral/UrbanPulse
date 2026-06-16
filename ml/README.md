# ML Workspace

This directory is used for local model development and offline data work. Most
training and backtesting still lives here. A saved delay-increase model from
this workspace is now loaded by the FastAPI service for minute-level realtime
inference.

The ML workspace has been built around exported vehicle-position history. The
first version used processed Parquet batches directly from CSV exports. The
newer shape uses a small DuckDB and Parquet lake so historical data can be
queried, feature datasets can be rebuilt, and model experiments can be repeated
with less manual glue.

## What Has Been Built

- VPS export helpers have been used to bring `vehicle_positions` history into
  the local workspace.
- A partitioned Parquet lake has been added under `ml/lake/`.
- DuckDB views have been added for local analytical queries over the lake.
- Delay feature datasets have been generated from the lake into
  `ml/data/features/`.
- A tabular delay-increase classifier has been trained as the first serious
  baseline before any GNN work is attempted.
- W&B logging has been added for training metrics, model artifacts, metadata,
  scored alert counts, and alert JSON artifacts.
- Scored alerts are written as JSON and served by the data API for the frontend
  risk panel.

## How It Works

The current offline path looks like this.

```text
VPS CSV exports -> DuckDB and Parquet lake -> delay features -> model training -> scored alerts
```

DuckDB is used as a local analytical engine. It is not replacing PostgreSQL in
the realtime app. PostgreSQL and TimescaleDB remain the operational store for
live collection, replay, and short-term history.

The preferred archive format is partitioned Parquet.

```text
ml/lake/vehicle_positions/service_date=YYYY-MM-DD/*.parquet
```

The local DuckDB file stores views and metadata around those Parquet files.

```text
ml/lake/urbanpulse.duckdb
```

The scored alert artifact is written here.

```text
ml/models/delay_increase_alerts.json
```

The artifact is still useful as an offline fallback. During live serving, recent
PostgreSQL/TimescaleDB rows are scored in the FastAPI service with the saved HGB
model, using the last few minutes of vehicle history as context.

## Reference Commands

The original export flow can still be reproduced with the helper scripts.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r ml/requirements.txt
./ml/scripts/fetch_vehicle_exports.sh
./ml/scripts/unpack_exports.sh ml/vehicle_positions_all_chunks.tar.gz
python ml/scripts/prepare_delay_baseline_dataset.py --all-batches
```

The DuckDB and Parquet lake is rebuilt from downloaded CSV exports with:

```bash
python ml/scripts/build_vehicle_positions_lake.py --force
```

A smaller smoke lake has been useful for checking the path quickly.

```bash
python ml/scripts/build_vehicle_positions_lake.py \
  --max-files 1 \
  --output-dir ml/lake_smoke/vehicle_positions \
  --database ml/lake_smoke/urbanpulse.duckdb \
  --manifest ml/lake_smoke/manifest.json \
  --force
```

The lake can be queried directly through DuckDB.

```bash
python - <<'PY'
import duckdb

con = duckdb.connect("ml/lake/urbanpulse.duckdb", read_only=True)
print(con.execute("""
    SELECT service_date, rows, vehicles
    FROM vehicle_positions_daily
    ORDER BY service_date
    LIMIT 5
""").fetchall())
PY
```

The delay feature dataset is built from the lake.

```bash
python ml/scripts/build_delay_features_from_lake.py
```

By default, the latest three available service dates are written here.

```text
ml/data/features/delay_5min_duckdb/service_date=YYYY-MM-DD/part.parquet
```

Specific windows can be rebuilt when a smaller or more controlled training
sample is needed.

```bash
python ml/scripts/build_delay_features_from_lake.py \
  --start-date 2026-05-01 \
  --end-date 2026-05-25 \
  --force
```

The current classifier baseline is trained from the DuckDB-built features.

```bash
python ml/scripts/train_delay_increase_classifier.py \
  --input-glob 'ml/data/features/delay_5min_duckdb/service_date=*/part.parquet' \
  --max-rows 1000000 \
  --selection spread \
  --threshold-seconds 60 \
  --min-precision 0.60
```

The older processed-batch path is still present and has been kept for comparison
while the lake path settles.

```bash
python ml/scripts/train_delay_baseline.py \
  --input-glob 'ml/data/processed/delay_baseline_5min_batch_*.parquet' \
  --max-rows 1000000 \
  --selection spread \
  --target delta

python ml/scripts/train_delay_increase_classifier.py \
  --input-glob 'ml/data/processed/delay_baseline_5min_batch_*.parquet' \
  --max-rows 1000000 \
  --selection spread \
  --threshold-seconds 60 \
  --min-precision 0.60
```

Large April and May exports have been processed one chunk at a time when memory
pressure appeared.

```bash
POLARS_MAX_THREADS=2 ./ml/scripts/process_vehicle_exports_one_by_one.sh
```

The scored alert JSON is produced from a saved model.

```bash
python ml/scripts/score_delay_increase.py \
  --model ml/models/delay_increase_hgb_5min.joblib \
  --input-glob 'ml/data/processed/delay_baseline_5min_recent_2026-*.parquet' \
  --max-rows 300000 \
  --latest-per-vehicle \
  --top-n 50 \
  --output-scope alerts \
  --output ml/models/delay_increase_alerts.json
```

## Experiment Tracking

W&B has been added as optional experiment tracking. When `WANDB_API_KEY` is
present in the repo-root `.env` file or the process environment, training and
scoring runs are logged to the configured project. The default project is
`urbanpulse`.

The training run logs row counts, positive rates, HGB metrics, top-risk
precision, high-confidence alert metrics, and the saved model files. The scoring
run logs scored row counts, alert counts, risk summaries, and the alert JSON
artifact used by the app.

W&B can be disabled for a run with:

```bash
WANDB_MODE=disabled python ml/scripts/train_delay_increase_classifier.py
```

## Current Modelling Read

The current targets are:

- regression of future delay or delay delta
- classification of whether delay increases by at least 60 seconds within five
  minutes

The most useful baseline so far has been `HistGradientBoostingClassifier`.
Regression has been harder because persistence is already a strong mean absolute
error baseline. Delay-increase classification currently looks more useful for a
first alerting feature.

The latest recent-data holdout used April and May data for training and
validation, with the newest May 25 file treated as an out-of-time test. The
representative holdout metrics were:

- ROC AUC around `0.737`
- average precision around `0.494`
- top 1 percent precision around `0.860`
- high-confidence test precision around `0.61`
- high-confidence recall around `0.25`

This is a benchmark for later GNN work, not a final production model.

## What Comes Next

- The Airflow DAG is now expected to become the main way this workflow is run.
- W&B is becoming the place where baseline runs are compared instead of
  relying only on console logs.
- The local lake is expected to be synced to object storage instead of living only
  on a laptop or VPS export.
- The tabular baseline is being kept as the benchmark for future graph models.
- GNN snapshots are expected to be built from route and stop topology plus
  recent vehicle state once the data quality and chronological validation setup
  has earned enough trust.

The project is intentionally baseline-first. If the dataset is weak or unstable,
the data shape is expected to improve before time is spent on more complex
models.
