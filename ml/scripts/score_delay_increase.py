from __future__ import annotations

import argparse
from glob import glob
from pathlib import Path

import joblib
import polars as pl

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score prepared delay baseline parquet rows with a saved classifier.",
    )
    parser.add_argument(
        "--model",
        default="ml/models/delay_increase_hgb_5min.joblib",
        help="Path to the saved joblib model artifact.",
    )
    parser.add_argument(
        "--input-glob",
        default="ml/data/processed/delay_baseline_5min_recent_2026-*.parquet",
        help="Glob for prepared parquet rows to score.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=300_000,
        help="Maximum rows to score after sorting by time. Use <=0 for all rows.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="Rows to print and optionally write after sorting by risk descending.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Alert threshold. Defaults to artifact metadata precision_threshold.",
    )
    parser.add_argument(
        "--latest-per-vehicle",
        action="store_true",
        help="Keep only the latest scored row per vehicle before ranking.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output path for the ranked rows (.parquet or .csv).",
    )
    parser.add_argument(
        "--output-scope",
        choices=["top", "alerts", "all"],
        default="top",
        help="Rows to write when --output is set.",
    )
    return parser.parse_args()


def resolve_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def resolve_input_paths(pattern: str) -> list[str]:
    resolved_pattern = str(resolve_path(pattern))
    matches = sorted(glob(resolved_pattern))
    if not matches:
        raise FileNotFoundError(f"No parquet files matched {resolved_pattern!r}")
    return matches


def load_artifact(path: Path) -> tuple[object, dict]:
    artifact = joblib.load(path)
    if not isinstance(artifact, dict) or "model" not in artifact:
        raise ValueError(f"{path} is not a supported delay-increase model artifact.")
    return artifact["model"], artifact.get("metadata", {})


def load_rows(paths: list[str], feature_columns: list[str], max_rows: int) -> pl.DataFrame:
    source = pl.scan_parquet(paths)
    schema_names = set(source.collect_schema().names())
    missing_features = [
        column
        for column in feature_columns
        if column not in schema_names
    ]
    if missing_features:
        raise ValueError(f"Missing feature columns: {', '.join(missing_features)}")

    context_columns = [
        "vehicle_id",
        "time",
        "delay",
        "speed",
        "line",
        "route_id",
        "state_position",
        "target_delay",
        "target_delay_delta",
    ]
    selected_columns = list(
        dict.fromkeys(
            column
            for column in [*context_columns, *feature_columns]
            if column in schema_names
        ),
    )

    frame = source.select(selected_columns).sort("time")
    if max_rows > 0:
        frame = frame.tail(max_rows)
    return frame.collect()


def add_scores(
    frame: pl.DataFrame,
    model,
    feature_columns: list[str],
    threshold: float,
) -> pl.DataFrame:
    scores = model.predict_proba(frame.select(feature_columns).to_pandas())[:, 1]
    scored = frame.with_columns(
        pl.Series("delay_increase_risk", scores),
        pl.Series("delay_increase_alert", scores >= threshold),
    )

    if "target_delay_delta" in scored.columns:
        scored = scored.with_columns(
            (pl.col("target_delay_delta") >= 60)
            .cast(pl.Int8)
            .alias("actual_increase_60s"),
        )

    return scored


def latest_per_vehicle(frame: pl.DataFrame) -> pl.DataFrame:
    if "vehicle_id" not in frame.columns or "time" not in frame.columns:
        return frame
    return (
        frame
        .sort(["vehicle_id", "time"])
        .group_by("vehicle_id")
        .last()
        .sort("delay_increase_risk", descending=True)
    )


def write_output(frame: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix == ".json":
        output_path.write_text(frame.write_json(), encoding="utf-8")
    elif output_path.suffix == ".csv":
        frame.write_csv(output_path)
    else:
        frame.write_parquet(output_path)
    print(f"\nWrote scored rows: {output_path}")


def output_frame_for_scope(
    scored: pl.DataFrame,
    top_rows: pl.DataFrame,
    scope: str,
) -> pl.DataFrame:
    if scope == "top":
        return top_rows
    if scope == "alerts":
        return scored.filter(pl.col("delay_increase_alert"))
    return scored


def main() -> None:
    args = parse_args()
    model_path = resolve_path(args.model)
    model, metadata = load_artifact(model_path)
    feature_columns = metadata.get("feature_columns")
    if not feature_columns:
        raise ValueError("Model artifact metadata does not include feature_columns.")

    threshold = args.threshold
    if threshold is None:
        threshold = metadata.get("precision_threshold")
    if threshold is None:
        threshold = 0.5

    input_paths = resolve_input_paths(args.input_glob)
    print(f"Model: {model_path}")
    print(f"Matched {len(input_paths)} input parquet file(s).")
    print(f"Threshold: {threshold:.3f}")

    frame = load_rows(input_paths, feature_columns, args.max_rows)
    scored = add_scores(frame, model, feature_columns, threshold)
    if args.latest_per_vehicle:
        scored = latest_per_vehicle(scored)
    else:
        scored = scored.sort("delay_increase_risk", descending=True)

    top_rows = scored.head(args.top_n)
    display_columns = [
        "delay_increase_risk",
        "delay_increase_alert",
        "vehicle_id",
        "time",
        "line",
        "route_id",
        "state_position",
        "delay",
        "speed",
        "target_delay_delta",
        "actual_increase_60s",
    ]
    display_columns = [
        column
        for column in display_columns
        if column in top_rows.columns
    ]
    print("\nTop scored rows")
    print(top_rows.select(display_columns))

    alert_count = int(scored["delay_increase_alert"].sum())
    print(
        f"\nScored rows: {scored.height}; "
        f"alerts at threshold: {alert_count} ({alert_count / scored.height:.3%})",
    )

    if args.output:
        output_frame = output_frame_for_scope(scored, top_rows, args.output_scope)
        write_output(output_frame, resolve_path(args.output))


if __name__ == "__main__":
    main()
