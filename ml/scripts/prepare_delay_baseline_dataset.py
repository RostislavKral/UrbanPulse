from __future__ import annotations

import argparse
from glob import glob
from pathlib import Path

import polars as pl


SCRIPT_PATH = Path(__file__).resolve()
ML_ROOT = SCRIPT_PATH.parents[1]
REPO_ROOT = SCRIPT_PATH.parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a baseline dataset for delay prediction at t+5min.",
    )
    parser.add_argument(
        "--input-glob",
        default="ml/data/raw/exports/*.csv.gz",
        help="Glob for exported vehicle_positions chunks.",
    )
    parser.add_argument(
        "--output",
        default="ml/data/processed/delay_baseline_5min.parquet",
        help="Output parquet path.",
    )
    parser.add_argument(
        "--sampling-interval",
        default="30s",
        help="Per-vehicle sampling interval before target construction.",
    )
    parser.add_argument(
        "--prediction-horizon",
        default="5m",
        help="Future horizon for the delay label.",
    )
    parser.add_argument(
        "--target-tolerance",
        default="10m",
        help="Maximum forward lookup distance for the future delay target.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=2,
        help="Maximum number of chunk files to include in one run.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="Zero-based index of the first matched chunk file to include.",
    )
    parser.add_argument(
        "--all-batches",
        action="store_true",
        help="Process all matched files in batches of --max-files and write one parquet per batch.",
    )
    return parser.parse_args()


def parse_duration_seconds(value: str) -> int:
    units = {
        "s": 1,
        "m": 60,
        "h": 3600,
    }
    suffix = value[-1].lower()
    if suffix not in units:
        raise ValueError(f"Unsupported duration suffix in {value!r}")

    amount = int(value[:-1])
    return amount * units[suffix]


def resolve_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def resolve_all_input_matches(pattern: str) -> list[str]:
    resolved_pattern = str(resolve_path(pattern))
    matches = sorted(glob(resolved_pattern))
    if not matches:
        raise FileNotFoundError(
            f"No input files matched {resolved_pattern!r}. "
            f"Unpack the archive into {ML_ROOT / 'data' / 'raw'} first.",
        )
    return matches


def resolve_input_glob(pattern: str, start_index: int, max_files: int) -> list[str]:
    matches = resolve_all_input_matches(pattern)

    if start_index < 0:
        raise ValueError("start-index must be >= 0")
    if start_index >= len(matches):
        raise ValueError(
            f"start-index {start_index} is out of range for {len(matches)} matched files.",
        )

    limited = matches[start_index : start_index + max_files]
    end_index = start_index + len(limited) - 1
    print(
        f"Matched {len(matches)} files, using indices {start_index}..{end_index} "
        f"({len(limited)} files).",
    )
    for match in limited:
        print(f" - {match}")
    return limited


def build_batch_output_path(base_output_path: Path, batch_start: int) -> Path:
    stem = base_output_path.stem
    suffix = "".join(base_output_path.suffixes)
    if suffix:
        stem = base_output_path.name.removesuffix(suffix)
    batch_name = f"{stem}_batch_{batch_start:02d}.parquet"
    return base_output_path.with_name(batch_name)


def build_dataset(
    input_paths: list[str],
    sampling_interval: str,
    prediction_horizon: str,
    target_tolerance: str,
) -> pl.LazyFrame:
    prediction_horizon_seconds = parse_duration_seconds(prediction_horizon)

    base = (
        pl.scan_csv(
            input_paths,
            schema_overrides={
                "time": pl.Utf8,
                "vehicle_id": pl.Utf8,
                "line": pl.Utf8,
                "delay": pl.Int32,
                "speed": pl.Float32,
                "route_id": pl.Utf8,
                "mode": pl.Utf8,
                "route_type": pl.Int16,
                "trip_id": pl.Utf8,
                "state_position": pl.Utf8,
                "origin_timestamp": pl.Utf8,
                "last_stop_id": pl.Utf8,
                "last_stop_sequence": pl.Int32,
                "last_stop_arrival_time": pl.Utf8,
                "last_stop_departure_time": pl.Utf8,
                "next_stop_id": pl.Utf8,
                "next_stop_sequence": pl.Int32,
                "next_stop_arrival_time": pl.Utf8,
                "next_stop_departure_time": pl.Utf8,
            },
            infer_schema_length=10000,
        )
        .with_columns(
            pl.col("time").str.to_datetime(strict=False, time_zone="UTC"),
            pl.col("origin_timestamp").str.to_datetime(strict=False, time_zone="UTC"),
            pl.col("last_stop_arrival_time").str.to_datetime(strict=False, time_zone="UTC"),
            pl.col("last_stop_departure_time").str.to_datetime(strict=False, time_zone="UTC"),
            pl.col("next_stop_arrival_time").str.to_datetime(strict=False, time_zone="UTC"),
            pl.col("next_stop_departure_time").str.to_datetime(strict=False, time_zone="UTC"),
            pl.col("delay").cast(pl.Int32, strict=False),
            pl.col("speed").cast(pl.Float32, strict=False),
            pl.col("route_type").cast(pl.Int16, strict=False),
            pl.col("last_stop_sequence").cast(pl.Int32, strict=False),
            pl.col("next_stop_sequence").cast(pl.Int32, strict=False),
        )
        .filter(
            pl.col("time").is_not_null()
            & pl.col("vehicle_id").is_not_null()
            & pl.col("delay").is_not_null()
        )
        .with_columns(
            pl.col("time").dt.truncate(sampling_interval).alias("time_bucket"),
            pl.col("time").dt.hour().alias("hour_of_day"),
            pl.col("time").dt.weekday().alias("day_of_week"),
        )
        .sort(["vehicle_id", "time"])
    )

    sampled = (
        base.group_by(["vehicle_id", "time_bucket"])
        .agg(
            pl.col("time").last().alias("time"),
            pl.col("delay").last().alias("delay"),
            pl.col("speed").last().alias("speed"),
            pl.col("mode").last().alias("mode"),
            pl.col("line").last().alias("line"),
            pl.col("route_id").last().alias("route_id"),
            pl.col("trip_id").last().alias("trip_id"),
            pl.col("state_position").last().alias("state_position"),
            pl.col("route_type").last().alias("route_type"),
            pl.col("origin_timestamp").last().alias("origin_timestamp"),
            pl.col("last_stop_id").last().alias("last_stop_id"),
            pl.col("last_stop_sequence").last().alias("last_stop_sequence"),
            pl.col("last_stop_arrival_time").last().alias("last_stop_arrival_time"),
            pl.col("last_stop_departure_time").last().alias("last_stop_departure_time"),
            pl.col("next_stop_id").last().alias("next_stop_id"),
            pl.col("next_stop_sequence").last().alias("next_stop_sequence"),
            pl.col("next_stop_arrival_time").last().alias("next_stop_arrival_time"),
            pl.col("next_stop_departure_time").last().alias("next_stop_departure_time"),
            pl.col("hour_of_day").last().alias("hour_of_day"),
            pl.col("day_of_week").last().alias("day_of_week"),
        )
        .sort(["vehicle_id", "time"])
        .with_columns(
            (pl.col("time") + pl.duration(seconds=prediction_horizon_seconds)).alias(
                "target_time",
            ),
            pl.col("delay").shift(1).over("vehicle_id").alias("delay_lag_1"),
            pl.col("delay").shift(2).over("vehicle_id").alias("delay_lag_2"),
            pl.col("delay").shift(3).over("vehicle_id").alias("delay_lag_3"),
            pl.col("speed").shift(1).over("vehicle_id").alias("speed_lag_1"),
            pl.col("speed").shift(2).over("vehicle_id").alias("speed_lag_2"),
            pl.col("speed").shift(3).over("vehicle_id").alias("speed_lag_3"),
        )
        .with_columns(
            (pl.col("delay") - pl.col("delay_lag_1")).alias("delay_delta_1"),
            (pl.col("speed") - pl.col("speed_lag_1")).alias("speed_delta_1"),
            (pl.col("next_stop_sequence") - pl.col("last_stop_sequence")).alias(
                "stop_sequence_gap",
            ),
            (pl.col("time") - pl.col("origin_timestamp"))
            .dt.total_seconds()
            .cast(pl.Float32)
            .alias("seconds_since_origin"),
            (pl.col("time") - pl.col("last_stop_arrival_time"))
            .dt.total_seconds()
            .cast(pl.Float32)
            .alias("seconds_since_last_stop_arrival"),
            (pl.col("time") - pl.col("last_stop_departure_time"))
            .dt.total_seconds()
            .cast(pl.Float32)
            .alias("seconds_since_last_stop_departure"),
            (pl.col("next_stop_arrival_time") - pl.col("time"))
            .dt.total_seconds()
            .cast(pl.Float32)
            .alias("seconds_until_next_stop_arrival"),
            (pl.col("next_stop_departure_time") - pl.col("time"))
            .dt.total_seconds()
            .cast(pl.Float32)
            .alias("seconds_until_next_stop_departure"),
            (pl.col("next_stop_departure_time") - pl.col("next_stop_arrival_time"))
            .dt.total_seconds()
            .cast(pl.Float32)
            .alias("scheduled_next_stop_dwell_seconds"),
            (
                (pl.col("delay") + pl.col("delay_lag_1") + pl.col("delay_lag_2")) / 3.0
            ).alias("delay_mean_3"),
            (
                (pl.col("speed") + pl.col("speed_lag_1") + pl.col("speed_lag_2")) / 3.0
            ).alias("speed_mean_3"),
        )
    )

    future = sampled.select(
        pl.col("vehicle_id"),
        pl.col("time").alias("future_time"),
        pl.col("delay").alias("target_delay"),
    ).sort("future_time")

    dataset = sampled.sort("target_time").join_asof(
        future,
        left_on="target_time",
        right_on="future_time",
        by="vehicle_id",
        strategy="forward",
        tolerance=target_tolerance,
    ).filter(
        pl.col("target_delay").is_not_null() & pl.col("delay_lag_1").is_not_null(),
    ).with_columns(
        (pl.col("target_delay") - pl.col("delay")).alias("target_delay_delta"),
    )

    return dataset.select(
        "vehicle_id",
        "time",
        "target_time",
        "future_time",
        "delay",
        "target_delay",
        "target_delay_delta",
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
        "trip_id",
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
    )


def main() -> None:
    args = parse_args()
    output_path = resolve_path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.all_batches:
        all_matches = resolve_all_input_matches(args.input_glob)
        print(
            f"Matched {len(all_matches)} files, processing in batches of {args.max_files}.",
        )
        batch_count = 0
        for batch_start in range(0, len(all_matches), args.max_files):
            input_paths = all_matches[batch_start : batch_start + args.max_files]
            batch_output_path = build_batch_output_path(output_path, batch_start)
            print(
                f"Batch {batch_count}: indices {batch_start}..{batch_start + len(input_paths) - 1}",
            )
            for match in input_paths:
                print(f" - {match}")

            dataset = build_dataset(
                input_paths=input_paths,
                sampling_interval=args.sampling_interval,
                prediction_horizon=args.prediction_horizon,
                target_tolerance=args.target_tolerance,
            )
            dataset.sink_parquet(batch_output_path)
            print(f"Wrote: {batch_output_path}")
            batch_count += 1
        return

    input_paths = resolve_input_glob(
        args.input_glob,
        args.start_index,
        args.max_files,
    )

    dataset = build_dataset(
        input_paths=input_paths,
        sampling_interval=args.sampling_interval,
        prediction_horizon=args.prediction_horizon,
        target_tolerance=args.target_tolerance,
    )

    dataset.sink_parquet(output_path)
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
