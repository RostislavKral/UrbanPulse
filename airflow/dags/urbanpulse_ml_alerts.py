from __future__ import annotations

import json
import os
import shlex
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG


def config_value(name: str, default: str) -> str:
    """Read an Airflow Variable first, then an environment variable."""

    return Variable.get(name.lower(), default_var=os.getenv(name, default))


def q(value: str) -> str:
    return shlex.quote(value)


REPO_DIR = config_value("URBANPULSE_REPO_DIR", "/opt/urbanpulse")
PYTHON_BIN = config_value("URBANPULSE_PYTHON_BIN", "python")
FETCH_EXPORTS = config_value("URBANPULSE_FETCH_EXPORTS", "false").lower() == "true"
REBUILD_LAKE = config_value("URBANPULSE_REBUILD_LAKE", "false").lower() == "true"
BUILD_LAKE_FROM_CSV = FETCH_EXPORTS or REBUILD_LAKE

LAKE_INPUT_GLOB = config_value(
    "URBANPULSE_LAKE_INPUT_GLOB",
    "ml/data/raw/vehicle_exports/vehicle_positions_*.csv.gz",
)
LAKE_OUTPUT_DIR = config_value("URBANPULSE_LAKE_OUTPUT_DIR", "ml/lake/vehicle_positions")
LAKE_DATABASE = config_value("URBANPULSE_LAKE_DATABASE", "ml/lake/urbanpulse.duckdb")
LAKE_MANIFEST = config_value("URBANPULSE_LAKE_MANIFEST", "ml/lake/vehicle_positions_manifest.json")
LAKE_MAX_FILES = config_value("URBANPULSE_LAKE_MAX_FILES", "0")

FEATURE_OUTPUT_DIR = config_value(
    "URBANPULSE_FEATURE_OUTPUT_DIR",
    "ml/data/features/delay_5min_duckdb",
)
FEATURE_MANIFEST = config_value(
    "URBANPULSE_FEATURE_MANIFEST",
    "ml/data/features/delay_5min_duckdb_manifest.json",
)
FEATURE_DATES = config_value("URBANPULSE_FEATURE_DATES", "")
FEATURE_START_DATE = config_value("URBANPULSE_FEATURE_START_DATE", "")
FEATURE_END_DATE = config_value("URBANPULSE_FEATURE_END_DATE", "")
FEATURE_LATEST_DATES = config_value("URBANPULSE_FEATURE_LATEST_DATES", "3")
FEATURE_ALL_DATES = config_value("URBANPULSE_FEATURE_ALL_DATES", "false").lower() == "true"

DUCKDB_MEMORY_LIMIT = config_value("URBANPULSE_DUCKDB_MEMORY_LIMIT", "8GB")
DUCKDB_THREADS = config_value("URBANPULSE_DUCKDB_THREADS", "2")
RUN_WINDOW_COMPARISON = (
    config_value("URBANPULSE_RUN_WINDOW_COMPARISON", "false").lower() == "true"
)
WINDOW_COMPARISON_WINDOWS = config_value(
    "URBANPULSE_WINDOW_COMPARISON_WINDOWS",
    "14,30,60,90,180",
)
WINDOW_COMPARISON_REPORT = config_value(
    "URBANPULSE_WINDOW_COMPARISON_REPORT",
    "ml/reports/hgb_train_window_comparison.json",
)

FEATURE_INPUT_GLOB = config_value(
    "URBANPULSE_TRAIN_INPUT_GLOB",
    "ml/data/features/delay_5min_duckdb/service_date=*/part.parquet",
)
MODEL_OUTPUT = config_value(
    "URBANPULSE_MODEL_OUTPUT",
    "ml/models/delay_increase_hgb_5min.joblib",
)
ALERT_OUTPUT = config_value(
    "URBANPULSE_ALERT_OUTPUT",
    "ml/models/delay_increase_alerts.json",
)
QUALITY_REPORT = config_value(
    "URBANPULSE_QUALITY_REPORT",
    "ml/reports/data_quality_latest.json",
)
REALTIME_EVAL_REPORT = config_value(
    "URBANPULSE_REALTIME_EVAL_REPORT",
    "ml/reports/realtime_delay_prediction_eval.json",
)


def repo_command(command: str) -> str:
    return f"set -euo pipefail\ncd {q(REPO_DIR)}\n{command}\n"


def feature_date_args() -> str:
    args: list[str] = []
    if FEATURE_DATES:
        args.extend(["--dates", FEATURE_DATES])
    elif FEATURE_START_DATE or FEATURE_END_DATE:
        if FEATURE_START_DATE:
            args.extend(["--start-date", FEATURE_START_DATE])
        if FEATURE_END_DATE:
            args.extend(["--end-date", FEATURE_END_DATE])
    elif FEATURE_ALL_DATES:
        args.append("--all-dates")
    else:
        args.extend(["--latest-dates", FEATURE_LATEST_DATES])
    return " ".join(q(arg) for arg in args)


def window_comparison_command() -> str:
    if not RUN_WINDOW_COMPARISON:
        return "echo 'URBANPULSE_RUN_WINDOW_COMPARISON=false; skipping HGB window comparison.'"

    return (
        f"{q(PYTHON_BIN)} ml/scripts/compare_hgb_train_windows.py "
        f"--input-glob {q(FEATURE_INPUT_GLOB)} "
        f"--windows {q(WINDOW_COMPARISON_WINDOWS)} "
        f"--report {q(WINDOW_COMPARISON_REPORT)} "
        f"--max-rows {q(config_value('URBANPULSE_TRAIN_MAX_ROWS', '1000000'))} "
        f"--selection {q(config_value('URBANPULSE_TRAIN_SELECTION', 'spread'))} "
        f"--threshold-seconds {q(config_value('URBANPULSE_DELAY_THRESHOLD_SECONDS', '60'))} "
        f"--min-precision {q(config_value('URBANPULSE_MIN_PRECISION', '0.60'))} "
        f"--alert-cooldown-minutes {q(config_value('URBANPULSE_ALERT_COOLDOWN_MINUTES', '15'))} "
        f"--train-row-cap {q(config_value('URBANPULSE_TRAIN_ROW_CAP', '300000'))} "
        f"--holdout-last-files {q(config_value('URBANPULSE_HOLDOUT_LAST_FILES', '1'))} "
        f"--holdout-test-max-rows {q(config_value('URBANPULSE_HOLDOUT_TEST_MAX_ROWS', '300000'))} "
        f"--learning-curve-rows {q(config_value('URBANPULSE_COMPARISON_LEARNING_CURVE_ROWS', ''))} "
        f"--hgb-max-iter {q(config_value('URBANPULSE_HGB_MAX_ITER', '200'))} "
        f"--hgb-max-depth {q(config_value('URBANPULSE_HGB_MAX_DEPTH', '6'))} "
        f"--hgb-max-bins {q(config_value('URBANPULSE_HGB_MAX_BINS', '127'))} "
    )


def validate_alert_artifact() -> None:
    output_path = Path(ALERT_OUTPUT)
    if not output_path.is_absolute():
        output_path = Path(REPO_DIR) / output_path

    if not output_path.exists():
        raise FileNotFoundError(f"Alert artifact was not created: {output_path}")

    records = json.loads(output_path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise TypeError(f"Alert artifact must contain a JSON array: {output_path}")

    print(f"Validated alert artifact: {output_path}")
    print(f"Alert rows: {len(records)}")


default_args = {
    "owner": "urbanpulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="urbanpulse_ml_alerts",
    description="Build the UrbanPulse DuckDB lake, train delay-risk model, and publish alerts.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=config_value("URBANPULSE_AIRFLOW_SCHEDULE", "@daily"),
    catchup=False,
    max_active_runs=1,
    tags=["urbanpulse", "ml", "duckdb", "alerts"],
    doc_md="""
    Orchestrates the offline UrbanPulse ML path:

    1. optionally fetch exported vehicle-position CSVs
    2. run DB migrations
    3. build or refresh the DuckDB/Parquet lake
    4. build delay-prediction features
    5. train the delay-increase classifier
    6. score alert rows for the frontend/API
    7. validate generated data, model, and alert artifacts
    """,
) as dag:
    fetch_vehicle_exports = BashOperator(
        task_id="fetch_vehicle_exports",
        bash_command=repo_command(
            "./ml/scripts/fetch_vehicle_exports.sh"
            if FETCH_EXPORTS
            else "echo 'URBANPULSE_FETCH_EXPORTS=false; using existing local exports.'"
        ),
    )

    migrate_operational_db = BashOperator(
        task_id="migrate_operational_db",
        bash_command=repo_command(f"{q(PYTHON_BIN)} apps/data-service/db_migrations.py"),
    )

    build_vehicle_positions_lake = BashOperator(
        task_id="build_vehicle_positions_lake",
        bash_command=repo_command(
            "\n".join(
                [
                    f"if [ {q(str(BUILD_LAKE_FROM_CSV).lower())} = 'true' ] || [ ! -d {q(LAKE_OUTPUT_DIR)} ]; then",
                    "  echo 'Building vehicle-position lake from CSV exports.'",
                    f"  {q(PYTHON_BIN)} ml/scripts/build_vehicle_positions_lake.py "
                    f"--input-glob {q(LAKE_INPUT_GLOB)} "
                    f"--output-dir {q(LAKE_OUTPUT_DIR)} "
                    f"--database {q(LAKE_DATABASE)} "
                    f"--manifest {q(LAKE_MANIFEST)} "
                    f"--max-files {q(LAKE_MAX_FILES)} "
                    f"--memory-limit {q(DUCKDB_MEMORY_LIMIT)} "
                    f"--threads {q(DUCKDB_THREADS)} "
                    + ("--incremental" if FETCH_EXPORTS else "--force"),
                    "else",
                    "  echo 'Lake already exists; refreshing DuckDB views only.'",
                    f"  {q(PYTHON_BIN)} ml/scripts/build_vehicle_positions_lake.py "
                    f"--input-glob {q(LAKE_INPUT_GLOB)} "
                    f"--output-dir {q(LAKE_OUTPUT_DIR)} "
                    f"--database {q(LAKE_DATABASE)} "
                    f"--manifest {q(LAKE_MANIFEST)} "
                    f"--max-files {q(LAKE_MAX_FILES)} "
                    f"--memory-limit {q(DUCKDB_MEMORY_LIMIT)} "
                    f"--threads {q(DUCKDB_THREADS)} "
                    "--views-only",
                    "fi",
                ]
            )
        ),
    )

    build_delay_features = BashOperator(
        task_id="build_delay_features",
        bash_command=repo_command(
            f"{q(PYTHON_BIN)} ml/scripts/build_delay_features_from_lake.py "
            f"--lake-dir {q(LAKE_OUTPUT_DIR)} "
            f"--output-dir {q(FEATURE_OUTPUT_DIR)} "
            f"--manifest {q(FEATURE_MANIFEST)} "
            f"{feature_date_args()} "
            f"--memory-limit {q(DUCKDB_MEMORY_LIMIT)} "
            f"--threads {q(DUCKDB_THREADS)} "
            "--force"
        ),
    )

    compare_hgb_train_windows = BashOperator(
        task_id="compare_hgb_train_windows",
        bash_command=repo_command(window_comparison_command()),
    )

    train_delay_increase_model = BashOperator(
        task_id="train_delay_increase_model",
        bash_command=repo_command(
            f"{q(PYTHON_BIN)} ml/scripts/train_delay_increase_classifier.py "
            f"--input-glob {q(FEATURE_INPUT_GLOB)} "
            f"--max-rows {q(config_value('URBANPULSE_TRAIN_MAX_ROWS', '1000000'))} "
            f"--selection {q(config_value('URBANPULSE_TRAIN_SELECTION', 'spread'))} "
            f"--threshold-seconds {q(config_value('URBANPULSE_DELAY_THRESHOLD_SECONDS', '60'))} "
            f"--min-precision {q(config_value('URBANPULSE_MIN_PRECISION', '0.60'))} "
            f"--alert-cooldown-minutes {q(config_value('URBANPULSE_ALERT_COOLDOWN_MINUTES', '15'))} "
            f"--train-row-cap {q(config_value('URBANPULSE_TRAIN_ROW_CAP', '300000'))} "
            f"--holdout-last-files {q(config_value('URBANPULSE_HOLDOUT_LAST_FILES', '1'))} "
            f"--holdout-test-max-rows {q(config_value('URBANPULSE_HOLDOUT_TEST_MAX_ROWS', '300000'))} "
            f"--train-window-days {q(config_value('URBANPULSE_TRAIN_WINDOW_DAYS', '30'))} "
            f"--train-window-end {q(config_value('URBANPULSE_TRAIN_WINDOW_END', ''))} "
            f"--learning-curve-rows {q(config_value('URBANPULSE_LEARNING_CURVE_ROWS', '50000,150000,300000'))} "
            f"--hgb-max-iter {q(config_value('URBANPULSE_HGB_MAX_ITER', '200'))} "
            f"--hgb-max-depth {q(config_value('URBANPULSE_HGB_MAX_DEPTH', '6'))} "
            f"--hgb-max-bins {q(config_value('URBANPULSE_HGB_MAX_BINS', '127'))} "
            f"--model-output {q(MODEL_OUTPUT)}"
        ),
    )

    score_delay_alerts = BashOperator(
        task_id="score_delay_alerts",
        bash_command=repo_command(
            f"{q(PYTHON_BIN)} ml/scripts/score_delay_increase.py "
            f"--model {q(MODEL_OUTPUT)} "
            f"--input-glob {q(FEATURE_INPUT_GLOB)} "
            f"--max-rows {q(config_value('URBANPULSE_SCORE_MAX_ROWS', '300000'))} "
            f"--top-n {q(config_value('URBANPULSE_SCORE_TOP_N', '50'))} "
            "--latest-per-vehicle "
            f"--output-scope {q(config_value('URBANPULSE_SCORE_OUTPUT_SCOPE', 'all'))} "
            f"--output {q(ALERT_OUTPUT)}"
        ),
    )

    validate_pipeline_quality = BashOperator(
        task_id="validate_pipeline_quality",
        bash_command=repo_command(
            f"{q(PYTHON_BIN)} ml/scripts/validate_pipeline_quality.py "
            f"--lake-manifest {q(LAKE_MANIFEST)} "
            f"--feature-manifest {q(FEATURE_MANIFEST)} "
            f"--model {q(MODEL_OUTPUT)} "
            f"--alerts {q(ALERT_OUTPUT)} "
            f"--report {q(QUALITY_REPORT)} "
            f"--min-lake-rows {q(config_value('URBANPULSE_MIN_LAKE_ROWS', '1000'))} "
            f"--min-feature-rows {q(config_value('URBANPULSE_MIN_FEATURE_ROWS', '1000'))} "
            f"--max-alert-row-age-hours {q(config_value('URBANPULSE_MAX_ALERT_ROW_AGE_HOURS', '48'))} "
            + (
                "--fail-on-zero-alerts"
                if config_value("URBANPULSE_FAIL_ON_ZERO_ALERTS", "false").lower() == "true"
                else ""
            )
        ),
    )

    validate_scored_alerts = PythonOperator(
        task_id="validate_scored_alerts",
        python_callable=validate_alert_artifact,
    )

    evaluate_realtime_predictions = BashOperator(
        task_id="evaluate_realtime_predictions",
        bash_command=repo_command(
            f"{q(PYTHON_BIN)} ml/scripts/evaluate_realtime_delay_predictions.py "
            f"--report {q(REALTIME_EVAL_REPORT)} "
            f"--lookback-hours {q(config_value('URBANPULSE_REALTIME_EVAL_LOOKBACK_HOURS', '24'))} "
            f"--horizon-minutes {q(config_value('URBANPULSE_REALTIME_EVAL_HORIZON_MINUTES', '5'))} "
            f"--match-tolerance-seconds {q(config_value('URBANPULSE_REALTIME_EVAL_MATCH_TOLERANCE_SECONDS', '90'))} "
            f"--positive-delta-seconds {q(config_value('URBANPULSE_REALTIME_EVAL_POSITIVE_DELTA_SECONDS', '60'))} "
            f"--min-labeled-rows {q(config_value('URBANPULSE_REALTIME_EVAL_MIN_LABELED_ROWS', '30'))} "
            f"--max-predictions {q(config_value('URBANPULSE_REALTIME_EVAL_MAX_PREDICTIONS', '20000'))} "
            f"--score-source {q(config_value('URBANPULSE_REALTIME_EVAL_SCORE_SOURCE', 'realtime'))} "
            + (
                "--fail-on-empty"
                if config_value("URBANPULSE_REALTIME_EVAL_FAIL_ON_EMPTY", "false").lower()
                == "true"
                else ""
            )
        ),
    )

    (
        fetch_vehicle_exports
        >> migrate_operational_db
        >> build_vehicle_positions_lake
        >> build_delay_features
        >> compare_hgb_train_windows
        >> train_delay_increase_model
        >> score_delay_alerts
        >> validate_pipeline_quality
        >> validate_scored_alerts
        >> evaluate_realtime_predictions
    )
