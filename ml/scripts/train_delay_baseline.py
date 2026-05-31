from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from glob import glob
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]


@dataclass(frozen=True)
class LoadStats:
    eligible_rows: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a first baseline model for delay prediction.",
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
        default=500_000,
        help="Maximum number of rows to collect. Use <=0 for all rows.",
    )
    parser.add_argument(
        "--selection",
        choices=["first", "last", "spread"],
        default="spread",
        help="How to choose parquet batches when more files match than --max-files.",
    )
    parser.add_argument(
        "--target",
        choices=["delay", "delta"],
        default="delay",
        help="Train on absolute future delay or future delay change.",
    )
    parser.add_argument(
        "--group-min-rows",
        type=int,
        default=5000,
        help="Minimum test rows required to print per-group metrics.",
    )
    parser.add_argument(
        "--group-top-n",
        type=int,
        default=10,
        help="Maximum groups to print per grouped evaluation.",
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

    chosen_indices: list[int] = []
    last_index = len(matches) - 1
    for i in range(max_files):
        index = round(i * last_index / (max_files - 1))
        if index not in chosen_indices:
            chosen_indices.append(index)

    candidate = 0
    while len(chosen_indices) < max_files and candidate < len(matches):
        if candidate not in chosen_indices:
            chosen_indices.append(candidate)
        candidate += 1

    chosen_indices.sort()
    return [matches[index] for index in chosen_indices]


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


def load_dataset(paths: list[str], max_rows: int | None) -> tuple[pl.DataFrame, LoadStats]:
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


def split_time_ordered(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    n = frame.height
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)
    return (
        frame.slice(0, train_end),
        frame.slice(train_end, val_end - train_end),
        frame.slice(val_end, n - val_end),
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
        f"({sample_rate:.3%}). Metrics are for the sampled evaluation rows.",
    )


def metric_block(y_true, y_pred) -> dict[str, float]:
    mae = mean_absolute_error(y_true, y_pred)
    rmse = math.sqrt(mean_squared_error(y_true, y_pred))
    return {
        "mae": float(mae),
        "rmse": float(rmse),
    }


def print_group_metrics(
    test_df: pl.DataFrame,
    persistence_pred,
    model_pred,
    group_column: str,
    min_rows: int,
    top_n: int,
) -> None:
    if group_column not in test_df.columns:
        return

    rows = []
    groups = (
        test_df.select(group_column)
        .drop_nulls()
        .group_by(group_column)
        .len()
        .sort("len", descending=True)
    )

    for group_value in groups[group_column].to_list():
        mask = (test_df[group_column] == group_value).to_numpy()
        count = int(mask.sum())
        if count < min_rows:
            continue

        y_true = test_df["target_delay"].to_numpy()[mask]
        persistence_metrics = metric_block(y_true, persistence_pred[mask])
        model_metrics = metric_block(y_true, model_pred[mask])
        rows.append(
            {
                "group": str(group_value),
                "rows": count,
                "p_mae": persistence_metrics["mae"],
                "m_mae": model_metrics["mae"],
                "p_rmse": persistence_metrics["rmse"],
                "m_rmse": model_metrics["rmse"],
            }
        )

        if len(rows) >= top_n:
            break

    if not rows:
        return

    print(f"\nPer-{group_column} test metrics")
    print("  group | rows | persistence MAE/RMSE | HGB MAE/RMSE")
    for row in rows:
        print(
            f"  {row['group']} | {row['rows']} | "
            f"{row['p_mae']:.2f}/{row['p_rmse']:.2f} | "
            f"{row['m_mae']:.2f}/{row['m_rmse']:.2f}",
        )


def available_columns(frame: pl.DataFrame, candidates: list[str]) -> list[str]:
    return [
        column
        for column in candidates
        if column in frame.columns and train_has_observed_values(frame, column)
    ]


def train_has_observed_values(frame: pl.DataFrame, column: str) -> bool:
    return frame[column].drop_nulls().len() > 0


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
                                max_categories=255,
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


def main() -> None:
    args = parse_args()
    paths = resolve_input_paths(args.input_glob, args.max_files, args.selection)
    frame, load_stats = load_dataset(paths, args.max_rows)

    if frame.height < 1000:
        raise ValueError(
            f"Dataset is too small after loading ({frame.height} rows). "
            "Use more files or a larger max-rows value.",
        )

    train_df, val_df, test_df = split_time_ordered(frame)
    print(
        f"Rows: total={frame.height}, train={train_df.height}, "
        f"val={val_df.height}, test={test_df.height}",
    )
    print_sampling_summary(frame, load_stats)

    numeric_columns = [
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
    ]
    categorical_candidates = [
        "mode",
        "line",
        "route_id",
        "state_position",
        "last_stop_id",
        "next_stop_id",
    ]

    numeric_columns = available_columns(train_df, numeric_columns)
    categorical_columns = available_columns(train_df, categorical_candidates)
    feature_columns = numeric_columns + categorical_columns

    if not feature_columns:
        raise ValueError("No usable feature columns found in the training data.")

    print(f"Target: {args.target}")
    print(f"Numeric features: {', '.join(numeric_columns)}")
    print(
        "Categorical features: "
        + (", ".join(categorical_columns) if categorical_columns else "none"),
    )

    X_train = train_df.select(feature_columns).to_pandas()
    X_val = val_df.select(feature_columns).to_pandas()
    X_test = test_df.select(feature_columns).to_pandas()

    target_column = "target_delay_delta" if args.target == "delta" else "target_delay"
    y_train = train_df[target_column].to_numpy()

    y_val_delay = val_df["target_delay"].to_numpy()
    y_test_delay = test_df["target_delay"].to_numpy()

    persistence_val = metric_block(y_val_delay, val_df["delay"].to_numpy())
    persistence_test = metric_block(y_test_delay, test_df["delay"].to_numpy())

    model = Pipeline(
        steps=[
            (
                "preprocess",
                build_tree_preprocessor(numeric_columns, categorical_columns),
            ),
            (
                "model",
                HistGradientBoostingRegressor(
                    learning_rate=0.05,
                    max_depth=6,
                    max_iter=200,
                    random_state=42,
                    categorical_features=categorical_feature_mask(
                        numeric_columns,
                        categorical_columns,
                    ),
                ),
            ),
        ]
    )
    ridge_model = Pipeline(
        steps=[
            (
                "preprocess",
                build_linear_preprocessor(numeric_columns, categorical_columns),
            ),
            ("model", Ridge(alpha=1.0)),
        ]
    )

    model.fit(X_train, y_train)
    model_val_pred = model.predict(X_val)
    model_test_pred = model.predict(X_test)
    ridge_model.fit(X_train, y_train)
    ridge_val_pred = ridge_model.predict(X_val)
    ridge_test_pred = ridge_model.predict(X_test)

    if args.target == "delta":
        model_val_delay_pred = val_df["delay"].to_numpy() + model_val_pred
        model_test_delay_pred = test_df["delay"].to_numpy() + model_test_pred
        ridge_val_delay_pred = val_df["delay"].to_numpy() + ridge_val_pred
        ridge_test_delay_pred = test_df["delay"].to_numpy() + ridge_test_pred
    else:
        model_val_delay_pred = model_val_pred
        model_test_delay_pred = model_test_pred
        ridge_val_delay_pred = ridge_val_pred
        ridge_test_delay_pred = ridge_test_pred

    model_val = metric_block(y_val_delay, model_val_delay_pred)
    model_test = metric_block(y_test_delay, model_test_delay_pred)
    ridge_val = metric_block(y_val_delay, ridge_val_delay_pred)
    ridge_test = metric_block(y_test_delay, ridge_test_delay_pred)

    print("\nPersistence baseline")
    print(f"  val  MAE={persistence_val['mae']:.2f} RMSE={persistence_val['rmse']:.2f}")
    print(f"  test MAE={persistence_test['mae']:.2f} RMSE={persistence_test['rmse']:.2f}")

    print("\nRidge baseline")
    print(f"  val  MAE={ridge_val['mae']:.2f} RMSE={ridge_val['rmse']:.2f}")
    print(f"  test MAE={ridge_test['mae']:.2f} RMSE={ridge_test['rmse']:.2f}")

    print("\nHistGradientBoosting baseline")
    print(f"  val  MAE={model_val['mae']:.2f} RMSE={model_val['rmse']:.2f}")
    print(f"  test MAE={model_test['mae']:.2f} RMSE={model_test['rmse']:.2f}")

    persistence_test_pred = test_df["delay"].to_numpy()
    print_group_metrics(
        test_df,
        persistence_test_pred,
        model_test_delay_pred,
        "state_position",
        args.group_min_rows,
        args.group_top_n,
    )
    print_group_metrics(
        test_df,
        persistence_test_pred,
        model_test_delay_pred,
        "line",
        args.group_min_rows,
        args.group_top_n,
    )


if __name__ == "__main__":
    main()
