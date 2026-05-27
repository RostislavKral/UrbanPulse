# ML Workspace

This directory is for local model development, not for runtime services.

Initial workflow:

1. Copy the exported `vehicle_positions` archive from the VPS into `ml/`.
2. Unpack the archive into `ml/data/raw/`.
3. Build a baseline delay dataset with `t+5min` targets.
4. Train simple baselines before attempting any GNN model.

Recommended first run:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r ml/requirements.txt
./ml/scripts/fetch_vehicle_exports.sh
./ml/scripts/unpack_exports.sh ml/vehicle_positions_all_chunks.tar.gz
python ml/scripts/prepare_delay_baseline_dataset.py --all-batches
```

`fetch_vehicle_exports.sh` downloads `vehicle_positions_*.csv.gz` from the VPS
and packs them locally into `ml/vehicle_positions_all_chunks.tar.gz`.

For large April/May exports, process raw chunks one at a time to avoid OOM:

```bash
POLARS_MAX_THREADS=2 ./ml/scripts/process_vehicle_exports_one_by_one.sh
```

The script skips existing outputs and writes
`ml/data/processed/delay_baseline_5min_recent_*.parquet`.

Default output:

- raw chunks: `ml/data/raw/`
- processed batches: `ml/data/processed/delay_baseline_5min_batch_*.parquet`

Useful commands:

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

Add `--max-files 4` when you want a faster smoke test. Without `--max-files`,
the training scripts use every matched parquet batch and then apply `--max-rows`.
Use `--selection last` with `--max-files` for recent-window checks.
Use `--max-rows 0` to disable row sampling.

Classifier diagnostics:

```bash
python ml/scripts/train_delay_increase_classifier.py \
  --input-glob 'ml/data/processed/delay_baseline_5min_batch_*.parquet' \
  --max-rows 1000000 \
  --threshold-seconds 60 \
  --min-precision 0.60 \
  --alert-cooldown-minutes 15 \
  --train-row-cap 300000 \
  --hgb-max-bins 127 \
  --learning-curve-rows 50000,100000,250000,500000
```

Recent holdout check after adding new parquet batches:

```bash
python ml/scripts/train_delay_increase_classifier.py \
  --input-glob 'ml/data/processed/delay_baseline_5min_batch_*.parquet' \
  --max-rows 1000000 \
  --threshold-seconds 60 \
  --min-precision 0.60 \
  --alert-cooldown-minutes 15 \
  --train-row-cap 300000 \
  --hgb-max-bins 127 \
  --holdout-last-files 1 \
  --holdout-test-max-rows 300000
```

Score prepared parquet rows with a saved model:

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

The data API serves this artifact at `GET /delay-increase-alerts`.
Docker Compose mounts `ml/models/` read-only into the API container.

Current targets:

- regression: predict `delay` or `delay delta` at `t + horizon`
- classification: predict whether delay increases by at least 60 seconds

Current sampling:

- keep one row per vehicle per 30-second bucket

Current read:

- exact delay regression is hard. Persistence is already a strong MAE baseline
- delay-increase classification looks more promising for a first useful model
- 5-minute horizon is cleaner than 10-minute horizon on the current data
- evaluate the classifier primarily with average precision, top-risk precision,
  high-confidence alert volume, and per-group slices
- use the learning curve before adding much more data. More history helps only
  if validation/test average precision and top-risk precision keep rising
- if training runs out of RAM, keep more chronological coverage with `--max-rows`
  but cap model-fitting rows with `--train-row-cap`. Lower `--hgb-max-bins`
  to reduce HGB memory pressure
- use `--holdout-last-files 1` after adding fresh data to train on older
  batches and evaluate on the newest batch as an out-of-time test

HGB baseline status:

- the useful classical ML baseline is `HistGradientBoostingClassifier`
- task: predict whether delay increases by at least 60 seconds within 5 minutes
- latest recent-data holdout used 24 April/May files for train/validation and
  the newest May 25 file as an out-of-time test
- representative holdout metrics: ROC AUC `0.737`, average precision `0.494`,
  top 1% precision `0.860`
- validation-selected high-confidence threshold was about `0.74`
- at that threshold, test precision was about `0.61` and recall about `0.25`
- this model is a benchmark for GNN work, not the final production model

GNN later:

- keep the HGB classifier as the tabular baseline and alerting benchmark
- build graph snapshots from route/stop topology plus recent vehicle state
- compare any GNN against the same chronological test windows and alert metrics

This is intentionally baseline-first. If this dataset is weak or unstable,
fix the data shape before spending time on graph models.
