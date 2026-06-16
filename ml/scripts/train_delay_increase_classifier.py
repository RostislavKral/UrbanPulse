from __future__ import annotations

import argparse
import gc
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from glob import glob
from pathlib import Path

import joblib
import numpy as np
import polars as pl
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

try:
    from .wandb_utils import init_wandb_run, log_file_artifact, log_metrics, log_table
except ImportError:
    from wandb_utils import init_wandb_run, log_file_artifact, log_metrics, log_table

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]


@dataclass(frozen=True)
class LoadStats:
    eligible_rows: int | None


@dataclass(frozen=True)
class TrainWindow:
    requested_days: int
    start: datetime | None
    end: datetime | None
    rows_before: int
    rows_after: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify whether delay increases beyond a threshold.",
    )
    parser.add_argument(
        "--input-glob",
        default="ml/data/processed/delay_baseline_5min_batch_*.parquet",
        help="Glob for prepared parquet batches.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Maximum number of parquet batches to include. Omit or use <=0 for all.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=1_000_000,
        help="Maximum number of rows to collect. Use <=0 for all rows.",
    )
    parser.add_argument(
        "--selection",
        choices=["first", "last", "spread"],
        default="spread",
        help="How to choose parquet batches when more files match than --max-files.",
    )
    parser.add_argument(
        "--threshold-seconds",
        type=int,
        default=60,
        help="Positive class: target_delay - delay >= this value.",
    )
    parser.add_argument(
        "--min-precision",
        type=float,
        default=0.50,
        help="Validation precision target for the high-confidence HGB threshold.",
    )
    parser.add_argument(
        "--include-logistic",
        action="store_true",
        help="Also train the slower logistic-regression sanity-check baseline.",
    )
    parser.add_argument(
        "--group-columns",
        default="state_position,line,hour_of_day",
        help="Comma-separated test-set columns for threshold diagnostics.",
    )
    parser.add_argument(
        "--group-min-rows",
        type=int,
        default=5000,
        help="Minimum test rows required to print per-group classifier metrics.",
    )
    parser.add_argument(
        "--group-top-n",
        type=int,
        default=10,
        help="Maximum groups to print for each grouped classifier diagnostic.",
    )
    parser.add_argument(
        "--alert-cooldown-minutes",
        type=int,
        default=15,
        help="Suppress repeated high-confidence event alerts per vehicle for this many minutes.",
    )
    parser.add_argument(
        "--learning-curve-rows",
        default="",
        help="Optional comma-separated train row counts to evaluate before the final fit.",
    )
    parser.add_argument(
        "--train-row-cap",
        type=int,
        default=0,
        help="Maximum train rows to use for model fitting. Use <=0 for all loaded train rows.",
    )
    parser.add_argument(
        "--holdout-last-files",
        type=int,
        default=0,
        help="Use the last N selected parquet files as an explicit chronological test set.",
    )
    parser.add_argument(
        "--holdout-test-max-rows",
        type=int,
        default=300_000,
        help="Maximum rows to collect from holdout test files. Use <=0 for all rows.",
    )
    parser.add_argument(
        "--train-window-days",
        type=int,
        default=0,
        help="Keep only the latest N days of train/validation rows. Use <=0 for all.",
    )
    parser.add_argument(
        "--train-window-end",
        default="",
        help=(
            "Optional ISO timestamp/date used as the rolling train-window end. "
            "Defaults to the max train/validation row time."
        ),
    )
    parser.add_argument(
        "--hgb-max-iter",
        type=int,
        default=200,
        help="HistGradientBoosting max_iter.",
    )
    parser.add_argument(
        "--hgb-max-depth",
        type=int,
        default=6,
        help="HistGradientBoosting max_depth.",
    )
    parser.add_argument(
        "--hgb-max-bins",
        type=int,
        default=255,
        help="HistGradientBoosting max_bins. Lower values use less memory.",
    )
    parser.add_argument(
        "--hgb-learning-rate",
        type=float,
        default=0.05,
        help="HistGradientBoosting learning_rate.",
    )
    parser.add_argument(
        "--model-output",
        default="",
        help="Optional path for a fitted joblib model artifact.",
    )
    parser.add_argument(
        "--artifact-aliases",
        default="latest",
        help="Comma-separated W&B artifact aliases for model outputs. Use empty to skip aliases.",
    )
    return parser.parse_args()


def resolve_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def select_paths(matches: list[str], max_files: int | None, selection: str) -> list[str]:
    if max_files is None or max_files <= 0:
        return matches

    if len(matches) <= max_files:
        return matches

    if selection == "first":
        return matches[:max_files]

    if selection == "last":
        return matches[-max_files:]

    if max_files == 1:
        return [matches[len(matches) // 2]]

    last_index = len(matches) - 1
    chosen = sorted({round(i * last_index / (max_files - 1)) for i in range(max_files)})
    candidate = 0
    while len(chosen) < max_files and candidate < len(matches):
        if candidate not in chosen:
            chosen.append(candidate)
        candidate += 1
    return [matches[index] for index in sorted(chosen)]


def resolve_input_paths(
    pattern: str,
    max_files: int | None,
    selection: str,
) -> list[str]:
    resolved_pattern = str(resolve_path(pattern))
    matches = sorted(glob(resolved_pattern))
    if not matches:
        raise FileNotFoundError(f"No parquet batches matched {resolved_pattern!r}")

    selected = select_paths(matches, max_files, selection)
    if max_files is None or max_files <= 0:
        selection_label = "all"
    else:
        selection_label = f"'{selection}' selection"
    print(f"Matched {len(matches)} parquet batches, using {len(selected)} via {selection_label}.")
    for match in selected:
        print(f" - {match}")
    return selected


def existing_columns(source: pl.LazyFrame, candidates: list[str]) -> list[str]:
    schema_names = set(source.collect_schema().names())
    return [column for column in candidates if column in schema_names]


def count_eligible_rows(path: str, threshold_seconds: int) -> int:
    source = pl.scan_parquet(path)
    required_columns = {"delay", "target_delay"}
    if not required_columns.issubset(source.collect_schema().names()):
        return 0

    return int(
        source
        .select(
            (
                pl.col("delay").is_not_null()
                & pl.col("target_delay").is_not_null()
                & ((pl.col("target_delay") - pl.col("delay")) >= threshold_seconds).is_not_null()
            )
            .sum()
            .alias("eligible_rows")
        )
        .collect()
        .item()
    )


def filter_eligible_paths(paths: list[str], threshold_seconds: int) -> list[str]:
    eligible_paths = []
    skipped_paths = []

    for path in paths:
        eligible_rows = count_eligible_rows(path, threshold_seconds)
        if eligible_rows > 0:
            eligible_paths.append(path)
        else:
            skipped_paths.append(path)

    if skipped_paths:
        print("\nSkipped empty feature parquet batch(es):")
        for path in skipped_paths:
            print(f" - {path}")

    if not eligible_paths:
        raise ValueError("No parquet batches contained eligible training rows.")

    return eligible_paths


def load_dataset(
    paths: list[str],
    max_rows: int | None,
    threshold_seconds: int,
) -> tuple[pl.DataFrame, LoadStats]:
    source = pl.scan_parquet(paths)
    required_columns = ["time", "delay", "target_delay"]
    missing_required = [
        column
        for column in required_columns
        if column not in source.collect_schema().names()
    ]
    if missing_required:
        raise ValueError(f"Missing required columns: {', '.join(missing_required)}")

    selected_columns = existing_columns(
        source,
        [
            "time",
            "vehicle_id",
            "delay",
            "target_delay",
            "speed",
            "delay_lag_1",
            "delay_lag_2",
            "delay_lag_3",
            "speed_lag_1",
            "speed_lag_2",
            "speed_lag_3",
            "delay_delta_1",
            "speed_delta_1",
            "delay_mean_3",
            "speed_mean_3",
            "mode",
            "line",
            "route_id",
            "state_position",
            "route_type",
            "last_stop_id",
            "last_stop_sequence",
            "next_stop_id",
            "next_stop_sequence",
            "stop_sequence_gap",
            "seconds_since_origin",
            "seconds_since_last_stop_arrival",
            "seconds_since_last_stop_departure",
            "seconds_until_next_stop_arrival",
            "seconds_until_next_stop_departure",
            "scheduled_next_stop_dwell_seconds",
            "hour_of_day",
            "day_of_week",
        ],
    )

    dataset = (
        source
        .select(
            selected_columns,
        )
        .with_columns(
            (pl.col("target_delay") - pl.col("delay")).alias("target_delay_delta"),
        )
        .filter(
            pl.col("delay").is_not_null()
            & pl.col("target_delay").is_not_null()
            & pl.col("target_delay_delta").is_not_null()
        )
        .with_columns(
            (pl.col("target_delay_delta") >= threshold_seconds)
            .cast(pl.Int8)
            .alias("target_increase"),
        )
    )

    eligible_rows = None
    if max_rows is not None and max_rows > 0:
        row_count = dataset.select(pl.len()).collect().item()
        eligible_rows = int(row_count)
        if row_count > max_rows:
            print(
                f"Eligible rows before cap: {row_count}; "
                f"collecting a deterministic spread sample of {max_rows}.",
            )
            sample_bucket = (pl.col("_sample_row") * max_rows / row_count).floor()
            previous_bucket = (
                (pl.col("_sample_row") - 1) * max_rows / row_count
            ).floor()
            dataset = (
                dataset
                .with_row_index("_sample_row")
                .filter(sample_bucket != previous_bucket)
                .drop("_sample_row")
                .limit(max_rows)
            )

    frame = dataset.collect()
    return frame.sort("time"), LoadStats(eligible_rows=eligible_rows)


def parse_csv_list(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def parse_row_counts(value: str) -> list[int]:
    row_counts = []
    for part in parse_csv_list(value):
        row_count = int(part)
        if row_count <= 0:
            raise ValueError("--learning-curve-rows values must be positive integers.")
        row_counts.append(row_count)
    return sorted(set(row_counts))


def split_time_ordered(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    n = frame.height
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)
    return (
        frame.slice(0, train_end),
        frame.slice(train_end, val_end - train_end),
        frame.slice(val_end, n - val_end),
    )


def split_train_val_ordered(
    frame: pl.DataFrame,
    val_fraction: float = 0.17647058823529413,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    n = frame.height
    val_rows = int(n * val_fraction)
    train_rows = n - val_rows
    return (
        frame.slice(0, train_rows),
        frame.slice(train_rows, val_rows),
    )


def format_time_window(frame: pl.DataFrame) -> str:
    if "time" not in frame.columns or frame.height == 0:
        return "n/a"

    start = frame["time"].min()
    end = frame["time"].max()
    return f"{start} -> {end}"


def normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def parse_window_end(value: str) -> datetime | None:
    if not value.strip():
        return None
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return normalize_datetime(parsed)


def filter_train_window(
    frame: pl.DataFrame,
    window_days: int,
    window_end: str,
) -> tuple[pl.DataFrame, TrainWindow]:
    if frame.height == 0:
        return frame, TrainWindow(window_days, None, None, 0, 0)

    requested_end = parse_window_end(window_end)
    end = requested_end or normalize_datetime(frame["time"].max())
    start = end - timedelta(days=window_days) if window_days > 0 else None
    filtered = frame
    if start is not None:
        filtered = filtered.filter(
            (pl.col("time") >= start)
            & (pl.col("time") <= end),
        )
        print(
            "\nApplied rolling train window: "
            f"{window_days}d ({start} -> {end}), "
            f"rows={filtered.height}/{frame.height}",
        )
    else:
        print("\nApplied rolling train window: all available train/validation rows.")

    return filtered, TrainWindow(
        requested_days=window_days,
        start=start,
        end=end,
        rows_before=frame.height,
        rows_after=filtered.height,
    )


def train_window_payload(window: TrainWindow) -> dict[str, int | str | None]:
    return {
        "requested_days": window.requested_days,
        "start": window.start.isoformat() if window.start else None,
        "end": window.end.isoformat() if window.end else None,
        "rows_before": window.rows_before,
        "rows_after": window.rows_after,
    }


def print_split_summary(
    train_df: pl.DataFrame,
    val_df: pl.DataFrame,
    test_df: pl.DataFrame,
) -> None:
    print("\nTime-ordered split windows")
    for name, split_df in [
        ("train", train_df),
        ("val", val_df),
        ("test", test_df),
    ]:
        positive_rate = float(split_df["target_increase"].mean())
        print(
            f"  {name:<5} rows={split_df.height} "
            f"positive_rate={positive_rate:.3f} "
            f"time={format_time_window(split_df)}",
        )


def print_sampling_summary(frame: pl.DataFrame, load_stats: LoadStats) -> None:
    if load_stats.eligible_rows is None:
        return

    sample_rate = frame.height / load_stats.eligible_rows
    if sample_rate >= 0.999:
        return

    print(
        "\nSampling note: "
        f"collected {frame.height}/{load_stats.eligible_rows} eligible rows "
        f"({sample_rate:.3%}). Alert counts are for the sampled evaluation rows; "
        "use a denser recent window for production alert-volume estimates.",
    )


def split_holdout_paths(
    paths: list[str],
    holdout_last_files: int,
) -> tuple[list[str], list[str]]:
    if holdout_last_files <= 0:
        return paths, []
    if holdout_last_files >= len(paths):
        raise ValueError(
            "--holdout-last-files must be smaller than the selected parquet file count.",
        )
    return paths[:-holdout_last_files], paths[-holdout_last_files:]


def print_path_block(label: str, paths: list[str]) -> None:
    print(f"\n{label}: {len(paths)} parquet file(s)")
    for path in paths:
        print(f" - {path}")


def observed_columns(frame: pl.DataFrame, candidates: list[str]) -> list[str]:
    return [
        column
        for column in candidates
        if column in frame.columns and frame[column].drop_nulls().len() > 0
    ]


def categorical_feature_mask(
    numeric_columns: list[str],
    categorical_columns: list[str],
) -> np.ndarray:
    return np.asarray(
        [False] * len(numeric_columns) + [True] * len(categorical_columns),
        dtype=bool,
    )


def build_tree_preprocessor(
    numeric_columns: list[str],
    categorical_columns: list[str],
    max_categories: int,
) -> ColumnTransformer:
    transformers = [
        (
            "numeric",
            Pipeline(
                steps=[
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler()),
                ]
            ),
            numeric_columns,
        ),
    ]

    if categorical_columns:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        (
                            "encoder",
                            OrdinalEncoder(
                                handle_unknown="use_encoded_value",
                                unknown_value=-1,
                                encoded_missing_value=-1,
                                max_categories=max_categories,
                            ),
                        ),
                    ]
                ),
                categorical_columns,
            ),
        )

    return ColumnTransformer(transformers=transformers)


def build_linear_preprocessor(
    numeric_columns: list[str],
    categorical_columns: list[str],
) -> ColumnTransformer:
    transformers = [
        (
            "numeric",
            Pipeline(
                steps=[
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler()),
                ]
            ),
            numeric_columns,
        ),
    ]

    if categorical_columns:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        (
                            "encoder",
                            OneHotEncoder(
                                handle_unknown="ignore",
                                sparse_output=False,
                            ),
                        ),
                    ]
                ),
                categorical_columns,
            ),
        )

    return ColumnTransformer(transformers=transformers)


def build_hgb_classifier(
    numeric_columns: list[str],
    categorical_columns: list[str],
    learning_rate: float,
    max_depth: int,
    max_iter: int,
    max_bins: int,
) -> Pipeline:
    return Pipeline(
        steps=[
            (
                "preprocess",
                build_tree_preprocessor(
                    numeric_columns,
                    categorical_columns,
                    max_bins,
                ),
            ),
            (
                "model",
                HistGradientBoostingClassifier(
                    learning_rate=learning_rate,
                    max_depth=max_depth,
                    max_iter=max_iter,
                    max_bins=max_bins,
                    random_state=42,
                    class_weight="balanced",
                    categorical_features=categorical_feature_mask(
                        numeric_columns,
                        categorical_columns,
                    ),
                ),
            ),
        ]
    )


def print_report(name: str, y_true, y_pred, y_score) -> None:
    print(f"\n{name}")
    print("Confusion matrix:")
    print(confusion_matrix(y_true, y_pred))
    print(classification_report(y_true, y_pred, digits=3, zero_division=0))
    predicted_positive_count = int(np.sum(y_pred))
    predicted_positive_rate = predicted_positive_count / len(y_pred)
    print(
        "Predicted positives: "
        f"{predicted_positive_count}/{len(y_pred)} ({predicted_positive_rate:.3%})",
    )
    try:
        print(f"ROC AUC: {roc_auc_score(y_true, y_score):.3f}")
    except ValueError:
        print("ROC AUC: n/a")
    try:
        print(f"Average precision: {average_precision_score(y_true, y_score):.3f}")
    except ValueError:
        print("Average precision: n/a")


def safe_roc_auc(y_true, y_score) -> float | None:
    try:
        return float(roc_auc_score(y_true, y_score))
    except ValueError:
        return None


def safe_average_precision(y_true, y_score) -> float | None:
    try:
        return float(average_precision_score(y_true, y_score))
    except ValueError:
        return None


def threshold_metrics(
    prefix: str,
    y_true: np.ndarray,
    y_score: np.ndarray,
    threshold: float,
) -> dict[str, float | int]:
    y_pred = (y_score >= threshold).astype(int)
    alert_count = int(np.sum(y_pred))
    return {
        f"{prefix}/threshold": float(threshold),
        f"{prefix}/precision": float(precision_score(y_true, y_pred, zero_division=0)),
        f"{prefix}/recall": float(recall_score(y_true, y_pred, zero_division=0)),
        f"{prefix}/f1": float(f1_score(y_true, y_pred, zero_division=0)),
        f"{prefix}/alerts": alert_count,
        f"{prefix}/alert_rate": alert_count / len(y_pred),
    }


def event_alert_summary(
    frame: pl.DataFrame,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    cooldown_minutes: int,
) -> dict[str, float | int]:
    event_mask = event_alert_mask(frame, y_pred, cooldown_minutes)
    event_count = int(event_mask.sum())
    positives = int(np.sum(y_true))
    return {
        "test/high_confidence_event_alerts": event_count,
        "test/high_confidence_event_precision": (
            float(np.mean(y_true[event_mask])) if event_count > 0 else 0.0
        ),
        "test/high_confidence_event_recall": (
            float(np.sum(y_true[event_mask]) / positives) if positives else 0.0
        ),
    }


def choose_best_f1_threshold(y_true, y_score) -> tuple[float, float, float, float]:
    best = (0.50, -1.0, 0.0, 0.0)
    for threshold in np.linspace(0.05, 0.95, 91):
        y_pred = (y_score >= threshold).astype(int)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        if f1 > best[1]:
            best = (float(threshold), float(f1), float(precision), float(recall))
    return best


def choose_precision_threshold(
    y_true,
    y_score,
    min_precision: float,
) -> tuple[float, float, float, float] | None:
    best = None
    for threshold in np.linspace(0.05, 0.95, 91):
        y_pred = (y_score >= threshold).astype(int)
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        if precision < min_precision:
            continue
        candidate = (float(threshold), float(f1), float(precision), float(recall))
        if best is None or candidate[3] > best[3]:
            best = candidate
    return best


def print_threshold_choice(label: str, threshold_data: tuple[float, float, float, float]) -> None:
    threshold, f1, precision, recall = threshold_data
    print(
        f"{label}: threshold={threshold:.2f}, "
        f"val_f1={f1:.3f}, val_precision={precision:.3f}, val_recall={recall:.3f}",
    )


def print_alert_volume(
    label: str,
    frame: pl.DataFrame,
    y_pred: np.ndarray,
) -> None:
    alert_count = int(np.sum(y_pred))
    row_count = len(y_pred)
    if row_count == 0:
        return

    pieces = [
        f"{alert_count}/{row_count} row-alerts ({alert_count / row_count:.3%})",
    ]

    if "vehicle_id" in frame.columns and alert_count > 0:
        vehicles = frame["vehicle_id"].to_numpy()
        pieces.append(f"{len(set(vehicles[y_pred.astype(bool)]))} vehicles")

    if "time" in frame.columns and frame.height > 1:
        start = frame["time"].min()
        end = frame["time"].max()
        span_seconds = (end - start).total_seconds()
        if span_seconds > 0:
            days = span_seconds / 86_400
            hours = span_seconds / 3_600
            pieces.append(f"{alert_count / days:.1f}/day")
            pieces.append(f"{alert_count / hours:.1f}/hour")

    print(f"{label} alert volume: " + ", ".join(pieces))


def print_score_bands(y_true: np.ndarray, y_score: np.ndarray) -> None:
    print("\nHGB score bands on test")
    print("  score band | rows | observed positive rate | avg score | lift")
    base_rate = float(np.mean(y_true))
    edges = np.linspace(0.0, 1.0, 11)
    for left, right in zip(edges[:-1], edges[1:]):
        if right == 1.0:
            mask = (y_score >= left) & (y_score <= right)
        else:
            mask = (y_score >= left) & (y_score < right)
        rows = int(mask.sum())
        if rows == 0:
            continue
        observed_rate = float(np.mean(y_true[mask]))
        avg_score = float(np.mean(y_score[mask]))
        lift = observed_rate / base_rate if base_rate > 0 else 0.0
        print(
            f"  {left:.1f}-{right:.1f} | {rows} | "
            f"{observed_rate:.3f} | {avg_score:.3f} | {lift:.2f}x",
        )


def print_top_risk_table(
    y_true: np.ndarray,
    y_score: np.ndarray,
    fractions: tuple[float, ...] = (0.005, 0.01, 0.02, 0.05, 0.10),
) -> None:
    print("\nTop-risk slices on test")
    print("  slice | rows | min score | precision | recall | lift")
    base_rate = float(np.mean(y_true))
    positives = int(np.sum(y_true))
    ranked_indices = np.argsort(y_score)[::-1]

    for fraction in fractions:
        rows = max(1, int(round(len(y_true) * fraction)))
        selected = ranked_indices[:rows]
        precision = float(np.mean(y_true[selected]))
        recall = float(np.sum(y_true[selected]) / positives) if positives > 0 else 0.0
        lift = precision / base_rate if base_rate > 0 else 0.0
        min_score = float(np.min(y_score[selected]))
        print(
            f"  top {fraction:.1%} | {rows} | {min_score:.3f} | "
            f"{precision:.3f} | {recall:.3f} | {lift:.2f}x",
        )


def top_fraction_precision(
    y_true: np.ndarray,
    y_score: np.ndarray,
    fraction: float,
) -> float:
    rows = max(1, int(round(len(y_true) * fraction)))
    ranked_indices = np.argsort(y_score)[::-1]
    selected = ranked_indices[:rows]
    return float(np.mean(y_true[selected]))


def spread_indices(total_rows: int, selected_rows: int) -> np.ndarray:
    if selected_rows >= total_rows:
        return np.arange(total_rows)
    return np.linspace(0, total_rows - 1, selected_rows, dtype=int)


def cap_training_frame(frame: pl.DataFrame, row_cap: int) -> pl.DataFrame:
    if row_cap <= 0 or frame.height <= row_cap:
        return frame
    return frame.sample(n=row_cap, seed=42).sort("time")


def print_learning_curve(
    row_counts: list[int],
    numeric_columns: list[str],
    categorical_columns: list[str],
    X_train,
    y_train: np.ndarray,
    X_val,
    y_val: np.ndarray,
    X_test,
    y_test: np.ndarray,
    min_precision: float,
    hgb_learning_rate: float,
    hgb_max_depth: int,
    hgb_max_iter: int,
    hgb_max_bins: int,
) -> list[dict[str, int | float | None]]:
    if not row_counts:
        return []

    print("\nHGB learning curve")
    print(
        "  train_rows | val_avg_precision | test_avg_precision | "
        "test_roc_auc | top1_precision | threshold | test_precision | "
        "test_recall | alerts",
    )
    rows = []
    seen_train_rows = set()
    for requested_rows in row_counts:
        train_rows = min(requested_rows, len(y_train))
        if train_rows in seen_train_rows:
            continue
        seen_train_rows.add(train_rows)
        indices = spread_indices(len(y_train), train_rows)
        model = build_hgb_classifier(
            numeric_columns,
            categorical_columns,
            hgb_learning_rate,
            hgb_max_depth,
            hgb_max_iter,
            hgb_max_bins,
        )
        try:
            model.fit(X_train.iloc[indices], y_train[indices])

            val_score = model.predict_proba(X_val)[:, 1]
            test_score = model.predict_proba(X_test)[:, 1]
            val_avg_precision = average_precision_score(y_val, val_score)
            test_avg_precision = average_precision_score(y_test, test_score)
            test_roc_auc = roc_auc_score(y_test, test_score)
            top1_precision = top_fraction_precision(y_test, test_score, 0.01)
            threshold_data = choose_precision_threshold(
                y_val,
                val_score,
                min_precision,
            )

            if threshold_data is None:
                threshold = "n/a"
                test_precision = "n/a"
                test_recall = "n/a"
                alerts = "n/a"
                threshold_value = None
                test_precision_value = None
                test_recall_value = None
                alerts_value = None
            else:
                threshold_value = threshold_data[0]
                y_pred = (test_score >= threshold_value).astype(int)
                test_precision_value = precision_score(
                    y_test,
                    y_pred,
                    zero_division=0,
                )
                test_recall_value = recall_score(y_test, y_pred, zero_division=0)
                alerts_value = int(np.sum(y_pred))
                threshold = f"{threshold_value:.2f}"
                test_precision = f"{test_precision_value:.3f}"
                test_recall = f"{test_recall_value:.3f}"
                alerts = str(alerts_value)

            rows.append(
                {
                    "train_rows": int(train_rows),
                    "val_avg_precision": float(val_avg_precision),
                    "test_avg_precision": float(test_avg_precision),
                    "test_roc_auc": float(test_roc_auc),
                    "top1_precision": float(top1_precision),
                    "threshold": (
                        None if threshold_value is None else float(threshold_value)
                    ),
                    "test_precision": (
                        None
                        if test_precision_value is None
                        else float(test_precision_value)
                    ),
                    "test_recall": (
                        None if test_recall_value is None else float(test_recall_value)
                    ),
                    "alerts": alerts_value,
                },
            )

            print(
                f"  {train_rows} | {val_avg_precision:.3f} | "
                f"{test_avg_precision:.3f} | {test_roc_auc:.3f} | "
                f"{top1_precision:.3f} | {threshold} | {test_precision} | "
                f"{test_recall} | {alerts}",
            )
        except MemoryError:
            print(f"  {train_rows} | stopped: MemoryError")
            break
        finally:
            del model
            gc.collect()
    return rows


def event_alert_mask(
    frame: pl.DataFrame,
    y_pred: np.ndarray,
    cooldown_minutes: int,
) -> np.ndarray:
    row_alert_mask = y_pred.astype(bool)
    if (
        cooldown_minutes <= 0
        or "vehicle_id" not in frame.columns
        or "time" not in frame.columns
    ):
        return row_alert_mask

    cooldown = timedelta(minutes=cooldown_minutes)
    event_mask = np.zeros(len(y_pred), dtype=bool)
    vehicle_ids = frame["vehicle_id"].to_list()
    times = frame["time"].to_list()
    last_alert_by_vehicle = {}

    for index in np.flatnonzero(row_alert_mask):
        vehicle_id = vehicle_ids[index]
        alert_time = times[index]
        if vehicle_id is None or alert_time is None:
            event_mask[index] = True
            continue

        last_alert_time = last_alert_by_vehicle.get(vehicle_id)
        if last_alert_time is None or alert_time - last_alert_time >= cooldown:
            event_mask[index] = True
            last_alert_by_vehicle[vehicle_id] = alert_time

    return event_mask


def print_event_alert_metrics(
    label: str,
    frame: pl.DataFrame,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    cooldown_minutes: int,
) -> None:
    event_mask = event_alert_mask(frame, y_pred, cooldown_minutes)
    event_count = int(event_mask.sum())
    row_alert_count = int(np.sum(y_pred))
    if event_count == 0:
        print(f"{label} event alerts: none")
        return

    positives = int(np.sum(y_true))
    event_precision = float(np.mean(y_true[event_mask]))
    event_recall = float(np.sum(y_true[event_mask]) / positives) if positives else 0.0
    pieces = [
        f"{event_count} event-alerts",
        f"{row_alert_count - event_count} row-alerts suppressed",
        f"precision={event_precision:.3f}",
        f"recall={event_recall:.3f}",
    ]

    if "vehicle_id" in frame.columns:
        vehicle_ids = frame["vehicle_id"].to_numpy()
        pieces.append(f"{len(set(vehicle_ids[event_mask]))} vehicles")

    if "time" in frame.columns and frame.height > 1:
        start = frame["time"].min()
        end = frame["time"].max()
        span_seconds = (end - start).total_seconds()
        if span_seconds > 0:
            pieces.append(f"{event_count / (span_seconds / 86_400):.1f}/day")
            pieces.append(f"{event_count / (span_seconds / 3_600):.1f}/hour")

    print(
        f"{label} event alerts "
        f"({cooldown_minutes}m vehicle cooldown): "
        + ", ".join(pieces),
    )


def print_group_threshold_metrics(
    test_df: pl.DataFrame,
    y_true: np.ndarray,
    y_score: np.ndarray,
    threshold: float,
    group_column: str,
    min_rows: int,
    top_n: int,
) -> None:
    if group_column not in test_df.columns:
        return

    groups = (
        test_df.select(group_column)
        .drop_nulls()
        .group_by(group_column)
        .len()
        .sort("len", descending=True)
    )
    if groups.is_empty():
        return

    rows = []
    y_pred = (y_score >= threshold).astype(int)
    for group_value in groups[group_column].to_list():
        mask = (test_df[group_column] == group_value).to_numpy()
        count = int(mask.sum())
        if count < min_rows:
            continue

        group_y = y_true[mask]
        group_score = y_score[mask]
        group_pred = y_pred[mask]
        positives = int(np.sum(group_y))
        alerts = int(np.sum(group_pred))
        precision = precision_score(group_y, group_pred, zero_division=0)
        recall = recall_score(group_y, group_pred, zero_division=0)
        average_precision = None
        if len(np.unique(group_y)) > 1:
            average_precision = float(average_precision_score(group_y, group_score))

        rows.append(
            {
                "group": str(group_value),
                "rows": count,
                "positive_rate": positives / count,
                "average_precision": average_precision,
                "precision": float(precision),
                "recall": float(recall),
                "alerts": alerts,
            }
        )
        if len(rows) >= top_n:
            break

    if not rows:
        return

    print(f"\nPer-{group_column} test metrics at threshold={threshold:.2f}")
    print("  group | rows | pos_rate | avg_precision | precision | recall | alerts")
    for row in rows:
        avg_precision = (
            "n/a"
            if row["average_precision"] is None
            else f"{row['average_precision']:.3f}"
        )
        print(
            f"  {row['group']} | {row['rows']} | {row['positive_rate']:.3f} | "
            f"{avg_precision} | {row['precision']:.3f} | "
            f"{row['recall']:.3f} | {row['alerts']}",
        )


def write_model_artifact(
    output_path: Path,
    model: Pipeline,
    metadata: dict,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model": model,
            "metadata": metadata,
        },
        output_path,
    )

    metadata_path = output_path.with_suffix(output_path.suffix + ".json")
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(f"\nWrote model artifact: {output_path}")
    print(f"Wrote model metadata: {metadata_path}")


def main() -> None:
    args = parse_args()
    paths = resolve_input_paths(args.input_glob, args.max_files, args.selection)
    paths = filter_eligible_paths(paths, args.threshold_seconds)
    train_val_paths, holdout_paths = split_holdout_paths(paths, args.holdout_last_files)
    wandb_run = init_wandb_run(
        repo_root=REPO_ROOT,
        job_type="train",
        tags=["urbanpulse", "delay-increase", "hgb"],
        config={
            "input_glob": args.input_glob,
            "selected_file_count": len(paths),
            "train_val_file_count": len(train_val_paths),
            "holdout_file_count": len(holdout_paths),
            "max_files": args.max_files,
            "max_rows": args.max_rows,
            "selection": args.selection,
            "threshold_seconds": args.threshold_seconds,
            "min_precision": args.min_precision,
            "include_logistic": args.include_logistic,
            "alert_cooldown_minutes": args.alert_cooldown_minutes,
            "train_row_cap": args.train_row_cap,
            "holdout_last_files": args.holdout_last_files,
            "holdout_test_max_rows": args.holdout_test_max_rows,
            "train_window_days": args.train_window_days,
            "train_window_end": args.train_window_end,
            "hgb_learning_rate": args.hgb_learning_rate,
            "hgb_max_depth": args.hgb_max_depth,
            "hgb_max_iter": args.hgb_max_iter,
            "hgb_max_bins": args.hgb_max_bins,
            "model_output": args.model_output,
            "artifact_aliases": args.artifact_aliases,
        },
    )

    if holdout_paths:
        print_path_block("Train/validation source", train_val_paths)
        print_path_block("Holdout test source", holdout_paths)
        frame, load_stats = load_dataset(
            train_val_paths,
            args.max_rows,
            args.threshold_seconds,
        )
        frame, train_window = filter_train_window(
            frame,
            args.train_window_days,
            args.train_window_end,
        )
        test_df, test_load_stats = load_dataset(
            holdout_paths,
            args.holdout_test_max_rows,
            args.threshold_seconds,
        )
        if frame.height < 1000:
            raise ValueError(
                f"Train/validation data is too small after loading ({frame.height} rows).",
            )
        if test_df.height < 1000:
            raise ValueError(
                f"Holdout test data is too small after loading ({test_df.height} rows).",
            )
        train_df, val_df = split_train_val_ordered(frame)
    else:
        frame, load_stats = load_dataset(paths, args.max_rows, args.threshold_seconds)
        frame, train_window = filter_train_window(
            frame,
            args.train_window_days,
            args.train_window_end,
        )
        test_load_stats = None
        if frame.height < 1000:
            raise ValueError(f"Dataset is too small after loading ({frame.height} rows).")
        train_df, val_df, test_df = split_time_ordered(frame)

    total_rows = train_df.height + val_df.height + test_df.height
    total_positives = (
        int(train_df["target_increase"].sum())
        + int(val_df["target_increase"].sum())
        + int(test_df["target_increase"].sum())
    )
    positive_rate = total_positives / total_rows
    print(
        f"Rows: total={total_rows}, "
        f"train={train_df.height}, "
        f"val={val_df.height}, test={test_df.height}",
    )
    print(f"Threshold: +{args.threshold_seconds}s delay increase")
    print(f"Positive rate: {positive_rate:.3f}")
    print_sampling_summary(frame, load_stats)
    if test_load_stats is not None:
        print_sampling_summary(test_df, test_load_stats)
    print_split_summary(train_df, val_df, test_df)

    fit_train_df = cap_training_frame(train_df, args.train_row_cap)
    if fit_train_df.height != train_df.height:
        print(
            f"\nFit train rows capped: {fit_train_df.height}/{train_df.height} "
            f"(positive_rate={float(fit_train_df['target_increase'].mean()):.3f})",
        )

    numeric_columns = observed_columns(
        fit_train_df,
        [
            "delay",
            "speed",
            "delay_lag_1",
            "delay_lag_2",
            "delay_lag_3",
            "speed_lag_1",
            "speed_lag_2",
            "speed_lag_3",
            "delay_delta_1",
            "speed_delta_1",
            "delay_mean_3",
            "speed_mean_3",
            "route_type",
            "last_stop_sequence",
            "next_stop_sequence",
            "stop_sequence_gap",
            "seconds_since_origin",
            "seconds_since_last_stop_arrival",
            "seconds_since_last_stop_departure",
            "seconds_until_next_stop_arrival",
            "seconds_until_next_stop_departure",
            "scheduled_next_stop_dwell_seconds",
            "hour_of_day",
            "day_of_week",
        ],
    )
    categorical_columns = observed_columns(
        fit_train_df,
        ["mode", "line", "route_id", "state_position", "last_stop_id", "next_stop_id"],
    )
    feature_columns = numeric_columns + categorical_columns
    if not feature_columns:
        raise ValueError("No usable feature columns found in the training data.")

    if wandb_run is not None:
        wandb_run.config.update(
            {
                "rows_total": total_rows,
                "rows_train": train_df.height,
                "rows_val": val_df.height,
                "rows_test": test_df.height,
                "positive_rate": positive_rate,
                "train_window": train_window_payload(train_window),
                "numeric_feature_count": len(numeric_columns),
                "categorical_feature_count": len(categorical_columns),
                "feature_count": len(feature_columns),
                "numeric_columns": numeric_columns,
                "categorical_columns": categorical_columns,
                "time_window_train": format_time_window(train_df),
                "time_window_val": format_time_window(val_df),
                "time_window_test": format_time_window(test_df),
            },
            allow_val_change=True,
        )

    print(f"Numeric features: {', '.join(numeric_columns)}")
    print(
        "Categorical features: "
        + (", ".join(categorical_columns) if categorical_columns else "none"),
    )

    X_train = fit_train_df.select(feature_columns).to_pandas()
    y_train = fit_train_df["target_increase"].to_numpy()
    X_val = val_df.select(feature_columns).to_pandas()
    y_val = val_df["target_increase"].to_numpy()
    X_test = test_df.select(feature_columns).to_pandas()
    y_test = test_df["target_increase"].to_numpy()

    persistence_val = (
        val_df["delay_delta_1"].fill_null(0).to_numpy() >= args.threshold_seconds
    ).astype(int)
    persistence_test = (
        test_df["delay_delta_1"].fill_null(0).to_numpy() >= args.threshold_seconds
    ).astype(int)

    learning_curve_rows = parse_row_counts(args.learning_curve_rows)
    learning_curve_metrics = print_learning_curve(
        learning_curve_rows,
        numeric_columns,
        categorical_columns,
        X_train,
        y_train,
        X_val,
        y_val,
        X_test,
        y_test,
        args.min_precision,
        args.hgb_learning_rate,
        args.hgb_max_depth,
        args.hgb_max_iter,
        args.hgb_max_bins,
    )
    log_table(wandb_run, "learning_curve", learning_curve_metrics)
    for row in learning_curve_metrics:
        log_metrics(
            wandb_run,
            {
                "learning_curve/val_avg_precision": row["val_avg_precision"],
                "learning_curve/test_avg_precision": row["test_avg_precision"],
                "learning_curve/test_roc_auc": row["test_roc_auc"],
                "learning_curve/top1_precision": row["top1_precision"],
                "learning_curve/threshold": row["threshold"],
                "learning_curve/test_precision": row["test_precision"],
                "learning_curve/test_recall": row["test_recall"],
                "learning_curve/alerts": row["alerts"],
            },
            step=int(row["train_rows"] or 0),
        )

    logistic = None
    if args.include_logistic:
        logistic = Pipeline(
            steps=[
                (
                    "preprocess",
                    build_linear_preprocessor(numeric_columns, categorical_columns),
                ),
                (
                    "model",
                    LogisticRegression(
                        class_weight="balanced",
                        max_iter=1000,
                    ),
                ),
            ]
        )
    hgb = build_hgb_classifier(
        numeric_columns,
        categorical_columns,
        args.hgb_learning_rate,
        args.hgb_max_depth,
        args.hgb_max_iter,
        args.hgb_max_bins,
    )

    if logistic is not None:
        logistic.fit(X_train, y_train)
    hgb.fit(X_train, y_train)
    hgb_val_score = hgb.predict_proba(X_val)[:, 1]
    hgb_test_score = hgb.predict_proba(X_test)[:, 1]
    best_f1_threshold = choose_best_f1_threshold(y_val, hgb_val_score)
    precision_threshold = choose_precision_threshold(
        y_val,
        hgb_val_score,
        args.min_precision,
    )

    print_report(
        "Persistence-change baseline (validation)",
        y_val,
        persistence_val,
        persistence_val,
    )
    print_report(
        "Persistence-change baseline (test)",
        y_test,
        persistence_test,
        persistence_test,
    )

    if logistic is not None:
        print_report(
            "Logistic regression (test)",
            y_test,
            logistic.predict(X_test),
            logistic.predict_proba(X_test)[:, 1],
        )
    print_report(
        "HistGradientBoosting classifier (test)",
        y_test,
        (hgb_test_score >= 0.50).astype(int),
        hgb_test_score,
    )
    print_score_bands(y_test, hgb_test_score)
    print_top_risk_table(y_test, hgb_test_score)

    print("\nHGB threshold tuning on validation")
    print_threshold_choice("Best F1", best_f1_threshold)
    best_f1_pred = (hgb_test_score >= best_f1_threshold[0]).astype(int)
    print_report(
        f"HistGradientBoosting tuned for F1 (test, threshold={best_f1_threshold[0]:.2f})",
        y_test,
        best_f1_pred,
        hgb_test_score,
    )

    if precision_threshold is None:
        print(f"No validation threshold reached precision >= {args.min_precision:.2f}")
        precision_threshold_value = None
    else:
        print_threshold_choice(
            f"Precision >= {args.min_precision:.2f}",
            precision_threshold,
        )
        precision_threshold_value = precision_threshold[0]
        precision_pred = (hgb_test_score >= precision_threshold[0]).astype(int)
        print_report(
            "HistGradientBoosting high-confidence "
            f"(test, threshold={precision_threshold[0]:.2f})",
            y_test,
            precision_pred,
            hgb_test_score,
        )
        print_alert_volume("High-confidence HGB", test_df, precision_pred)
        print_event_alert_metrics(
            "High-confidence HGB",
            test_df,
            y_test,
            precision_pred,
            args.alert_cooldown_minutes,
        )
        for group_column in parse_csv_list(args.group_columns):
            print_group_threshold_metrics(
                test_df,
                y_test,
                hgb_test_score,
                precision_threshold[0],
                group_column,
                args.group_min_rows,
                args.group_top_n,
            )

    wandb_metrics = {
        "rows/total": total_rows,
        "rows/train": train_df.height,
        "rows/fit_train": fit_train_df.height,
        "rows/val": val_df.height,
        "rows/test": test_df.height,
        "train_window/requested_days": train_window.requested_days,
        "train_window/rows_before": train_window.rows_before,
        "train_window/rows_after": train_window.rows_after,
        "positive_rate/overall": positive_rate,
        "positive_rate/fit_train": float(fit_train_df["target_increase"].mean()),
        "positive_rate/val": float(val_df["target_increase"].mean()),
        "positive_rate/test": float(test_df["target_increase"].mean()),
        "val/hgb_roc_auc": safe_roc_auc(y_val, hgb_val_score),
        "val/hgb_average_precision": safe_average_precision(y_val, hgb_val_score),
        "test/hgb_roc_auc": safe_roc_auc(y_test, hgb_test_score),
        "test/hgb_average_precision": safe_average_precision(y_test, hgb_test_score),
        "test/top_0_5_percent_precision": top_fraction_precision(
            y_test,
            hgb_test_score,
            0.005,
        ),
        "test/top_1_percent_precision": top_fraction_precision(
            y_test,
            hgb_test_score,
            0.01,
        ),
        "test/top_5_percent_precision": top_fraction_precision(
            y_test,
            hgb_test_score,
            0.05,
        ),
    }
    wandb_metrics.update(threshold_metrics("test/hgb_default", y_test, hgb_test_score, 0.5))
    wandb_metrics.update(
        threshold_metrics(
            "test/best_f1",
            y_test,
            hgb_test_score,
            best_f1_threshold[0],
        )
    )
    if precision_threshold_value is not None:
        precision_pred = (hgb_test_score >= precision_threshold_value).astype(int)
        wandb_metrics.update(
            threshold_metrics(
                "test/high_confidence",
                y_test,
                hgb_test_score,
                precision_threshold_value,
            )
        )
        wandb_metrics.update(
            event_alert_summary(
                test_df,
                y_test,
                precision_pred,
                args.alert_cooldown_minutes,
            )
        )
    log_metrics(wandb_run, wandb_metrics)

    if args.model_output:
        model_output_path = resolve_path(args.model_output)
        metadata = {
            "target": "target_delay_delta >= threshold_seconds",
            "threshold_seconds": args.threshold_seconds,
            "min_precision": args.min_precision,
            "best_f1_threshold": best_f1_threshold[0],
            "precision_threshold": precision_threshold_value,
            "numeric_columns": numeric_columns,
            "categorical_columns": categorical_columns,
            "feature_columns": feature_columns,
            "hgb": {
                "learning_rate": args.hgb_learning_rate,
                "max_depth": args.hgb_max_depth,
                "max_iter": args.hgb_max_iter,
                "max_bins": args.hgb_max_bins,
            },
            "rows": {
                "train": train_df.height,
                "fit_train": fit_train_df.height,
                "val": val_df.height,
                "test": test_df.height,
            },
            "positive_rates": {
                "fit_train": float(fit_train_df["target_increase"].mean()),
                "val": float(val_df["target_increase"].mean()),
                "test": float(test_df["target_increase"].mean()),
            },
            "time_windows": {
                "fit_train": format_time_window(fit_train_df),
                "val": format_time_window(val_df),
                "test": format_time_window(test_df),
            },
            "train_window": train_window_payload(train_window),
            "metrics": wandb_metrics,
            "input_paths": paths,
            "holdout_last_files": args.holdout_last_files,
        }
        write_model_artifact(model_output_path, hgb, metadata)
        artifact_aliases = parse_csv_list(args.artifact_aliases) or None
        log_file_artifact(
            wandb_run,
            model_output_path,
            name="delay-increase-hgb-model",
            artifact_type="model",
            aliases=artifact_aliases,
        )
        log_file_artifact(
            wandb_run,
            model_output_path.with_suffix(model_output_path.suffix + ".json"),
            name="delay-increase-hgb-metadata",
            artifact_type="metadata",
            aliases=artifact_aliases,
        )

    if wandb_run is not None:
        wandb_run.finish()


if __name__ == "__main__":
    main()
