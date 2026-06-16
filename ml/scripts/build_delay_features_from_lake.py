from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build delay-prediction feature parquet files from the DuckDB/Parquet lake.",
    )
    parser.add_argument(
        "--lake-dir",
        default="ml/lake/vehicle_positions",
        help="Partitioned vehicle_positions Parquet lake directory.",
    )
    parser.add_argument(
        "--output-dir",
        default="ml/data/features/delay_5min_duckdb",
        help="Output directory for feature parquet files.",
    )
    parser.add_argument(
        "--manifest",
        default="ml/data/features/delay_5min_duckdb_manifest.json",
        help="Manifest path for the generated feature dataset.",
    )
    parser.add_argument(
        "--dates",
        default="",
        help="Comma-separated service dates to build, for example 2026-05-19,2026-05-20.",
    )
    parser.add_argument("--start-date", default="", help="Inclusive service-date lower bound.")
    parser.add_argument("--end-date", default="", help="Inclusive service-date upper bound.")
    parser.add_argument(
        "--latest-dates",
        type=int,
        default=3,
        help="Build the latest N available lake dates when no date bounds are provided.",
    )
    parser.add_argument(
        "--all-dates",
        action="store_true",
        help="Build every service date in the lake.",
    )
    parser.add_argument(
        "--sampling-seconds",
        type=int,
        default=30,
        help="Per-vehicle sampling bucket in seconds.",
    )
    parser.add_argument(
        "--prediction-horizon-seconds",
        type=int,
        default=300,
        help="Future delay target horizon in seconds.",
    )
    parser.add_argument(
        "--target-tolerance-seconds",
        type=int,
        default=600,
        help="Maximum forward lookup tolerance for target delay.",
    )
    parser.add_argument(
        "--memory-limit",
        default="8GB",
        help="DuckDB memory limit.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=max((os.cpu_count() or 2) - 1, 1),
        help="DuckDB worker threads.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace existing output files for selected dates.",
    )
    return parser.parse_args()


def resolve_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def configure_duckdb(con: duckdb.DuckDBPyConnection, threads: int, memory_limit: str) -> None:
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = {sql_string(memory_limit)}")
    con.execute("LOAD parquet")


def lake_glob(lake_dir: Path) -> str:
    return str(lake_dir / "**" / "*.parquet")


def available_dates(con: duckdb.DuckDBPyConnection, lake_dir: Path) -> list[str]:
    return [
        row[0].isoformat()
        for row in con.execute(
            f"""
            SELECT DISTINCT service_date
            FROM read_parquet({sql_string(lake_glob(lake_dir))}, hive_partitioning = true)
            ORDER BY service_date
            """
        ).fetchall()
    ]


def parse_date_list(raw: str) -> list[str]:
    if not raw.strip():
        return []
    parsed = []
    for item in raw.split(","):
        value = item.strip()
        if not value:
            continue
        parsed.append(date.fromisoformat(value).isoformat())
    return parsed


def select_dates(args: argparse.Namespace, all_dates: list[str]) -> list[str]:
    explicit_dates = parse_date_list(args.dates)
    if explicit_dates:
        missing = sorted(set(explicit_dates) - set(all_dates))
        if missing:
            raise ValueError(f"Requested dates are not in the lake: {', '.join(missing)}")
        return explicit_dates

    if args.start_date or args.end_date:
        start = date.fromisoformat(args.start_date).isoformat() if args.start_date else all_dates[0]
        end = date.fromisoformat(args.end_date).isoformat() if args.end_date else all_dates[-1]
        return [value for value in all_dates if start <= value <= end]

    if args.all_dates:
        return all_dates

    return all_dates[-args.latest_dates :]


def output_path_for_date(output_dir: Path, service_date: str) -> Path:
    return output_dir / f"service_date={service_date}" / "part.parquet"


def feature_sql(
    lake_dir: Path,
    service_date: str,
    sampling_seconds: int,
    prediction_horizon_seconds: int,
    target_tolerance_seconds: int,
) -> str:
    source = sql_string(lake_glob(lake_dir))
    service_date_sql = f"DATE {sql_string(service_date)}"
    sampling_interval = sql_string(f"{sampling_seconds} seconds")
    horizon_interval = sql_string(f"{prediction_horizon_seconds} seconds")
    tolerance_interval = sql_string(f"{target_tolerance_seconds} seconds")
    lookahead_seconds = prediction_horizon_seconds + target_tolerance_seconds
    lookahead_interval = sql_string(f"{lookahead_seconds} seconds")

    return f"""
    WITH raw AS (
        SELECT *
        FROM read_parquet({source}, hive_partitioning = true)
        WHERE
            service_date BETWEEN {service_date_sql} - INTERVAL '1 day'
                AND {service_date_sql} + INTERVAL '1 day'
            AND time >= cast({service_date_sql} AS TIMESTAMP) - INTERVAL '15 minutes'
            AND time < cast({service_date_sql} AS TIMESTAMP)
                + INTERVAL '1 day'
                + INTERVAL {lookahead_interval}
            AND vehicle_id IS NOT NULL
            AND delay IS NOT NULL
            AND lat BETWEEN 49.9 AND 50.2
            AND lon BETWEEN 14.2 AND 14.75
    ),
    sampled AS (
        SELECT
            vehicle_id,
            time_bucket(INTERVAL {sampling_interval}, time) AS time_bucket,
            arg_max(time, time) AS time,
            arg_max(delay, time) AS delay,
            arg_max(speed, time) AS speed,
            arg_max(lat, time) AS lat,
            arg_max(lon, time) AS lon,
            arg_max(mode, time) AS mode,
            arg_max(line, time) AS line,
            arg_max(route_id, time) AS route_id,
            arg_max(trip_id, time) AS trip_id,
            arg_max(state_position, time) AS state_position,
            arg_max(route_type, time) AS route_type,
            arg_max(origin_timestamp, time) AS origin_timestamp,
            arg_max(last_stop_id, time) AS last_stop_id,
            arg_max(last_stop_sequence, time) AS last_stop_sequence,
            arg_max(last_stop_arrival_time, time) AS last_stop_arrival_time,
            arg_max(last_stop_departure_time, time) AS last_stop_departure_time,
            arg_max(next_stop_id, time) AS next_stop_id,
            arg_max(next_stop_sequence, time) AS next_stop_sequence,
            arg_max(next_stop_arrival_time, time) AS next_stop_arrival_time,
            arg_max(next_stop_departure_time, time) AS next_stop_departure_time
        FROM raw
        GROUP BY vehicle_id, time_bucket
    ),
    features AS (
        SELECT
            *,
            cast(time AS DATE) AS service_date,
            date_part('hour', time)::INTEGER AS hour_of_day,
            date_part('dayofweek', time)::INTEGER AS day_of_week,
            time + INTERVAL {horizon_interval} AS target_time,
            lag(delay, 1) OVER vehicle_window AS delay_lag_1,
            lag(delay, 2) OVER vehicle_window AS delay_lag_2,
            lag(delay, 3) OVER vehicle_window AS delay_lag_3,
            lag(speed, 1) OVER vehicle_window AS speed_lag_1,
            lag(speed, 2) OVER vehicle_window AS speed_lag_2,
            lag(speed, 3) OVER vehicle_window AS speed_lag_3,
            lag(lat, 1) OVER vehicle_window AS lat_lag_1,
            lag(lon, 1) OVER vehicle_window AS lon_lag_1
        FROM sampled
        WINDOW vehicle_window AS (PARTITION BY vehicle_id ORDER BY time)
    ),
    joined AS (
        SELECT
            f.*,
            target.future_time,
            target.target_delay
        FROM features f
        ASOF LEFT JOIN (
            SELECT
                vehicle_id,
                time AS future_time,
                delay AS target_delay
            FROM features
            ORDER BY vehicle_id, future_time
        ) target
        ON f.vehicle_id = target.vehicle_id
            AND f.target_time <= target.future_time
    ),
    labelled AS (
        SELECT
            *,
            future_time <= target_time + INTERVAL {tolerance_interval} AS target_is_within_tolerance
        FROM joined
    )
    SELECT
        vehicle_id,
        time,
        cast(time AS DATE) AS service_date,
        target_time,
        CASE
            WHEN target_is_within_tolerance THEN future_time
            ELSE NULL
        END AS future_time,
        CASE
            WHEN target_is_within_tolerance THEN date_diff('second', target_time, future_time)
            ELSE NULL
        END AS target_lookup_lag_seconds,
        delay,
        CASE
            WHEN target_is_within_tolerance THEN target_delay
            ELSE NULL
        END AS target_delay,
        CASE
            WHEN target_is_within_tolerance THEN target_delay - delay
            ELSE NULL
        END AS target_delay_delta,
        speed,
        lat,
        lon,
        lat_lag_1,
        lon_lag_1,
        delay_lag_1,
        delay_lag_2,
        delay_lag_3,
        speed_lag_1,
        speed_lag_2,
        speed_lag_3,
        delay - delay_lag_1 AS delay_delta_1,
        speed - speed_lag_1 AS speed_delta_1,
        (delay + delay_lag_1 + delay_lag_2) / 3.0 AS delay_mean_3,
        (speed + speed_lag_1 + speed_lag_2) / 3.0 AS speed_mean_3,
        mode,
        line,
        route_id,
        trip_id,
        state_position,
        route_type,
        last_stop_id,
        last_stop_sequence,
        next_stop_id,
        next_stop_sequence,
        next_stop_sequence - last_stop_sequence AS stop_sequence_gap,
        date_diff('second', origin_timestamp, time) AS seconds_since_origin,
        date_diff('second', last_stop_arrival_time, time) AS seconds_since_last_stop_arrival,
        date_diff('second', last_stop_departure_time, time) AS seconds_since_last_stop_departure,
        date_diff('second', time, next_stop_arrival_time) AS seconds_until_next_stop_arrival,
        date_diff('second', time, next_stop_departure_time) AS seconds_until_next_stop_departure,
        date_diff(
            'second',
            next_stop_arrival_time,
            next_stop_departure_time
        ) AS scheduled_next_stop_dwell_seconds,
        hour_of_day,
        day_of_week
    FROM labelled
    WHERE
        service_date = {service_date_sql}
        AND delay_lag_1 IS NOT NULL
    """


def write_features_for_date(
    con: duckdb.DuckDBPyConnection,
    lake_dir: Path,
    output_dir: Path,
    service_date: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    output_path = output_path_for_date(output_dir, service_date)
    if output_path.exists() and not args.force:
        print(f"Skipping existing {output_path}")
    else:
        if output_path.parent.exists() and args.force:
            shutil.rmtree(output_path.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        query = feature_sql(
            lake_dir=lake_dir,
            service_date=service_date,
            sampling_seconds=args.sampling_seconds,
            prediction_horizon_seconds=args.prediction_horizon_seconds,
            target_tolerance_seconds=args.target_tolerance_seconds,
        )
        con.execute(
            f"""
            COPY ({query})
            TO {sql_string(str(output_path))}
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

    row = con.execute(
        f"""
        SELECT
            count(*) AS row_count,
            count(DISTINCT vehicle_id) AS vehicles,
            cast(min(time) AS VARCHAR) AS min_time,
            cast(max(time) AS VARCHAR) AS max_time,
            avg(target_delay_delta) AS avg_target_delay_delta,
            sum(CASE WHEN target_delay_delta >= 60 THEN 1 ELSE 0 END) AS delay_increase_rows
        FROM read_parquet({sql_string(str(output_path))})
        """
    ).fetchone()

    result = {
        "service_date": service_date,
        "path": str(output_path),
        "bytes": output_path.stat().st_size,
        "rows": row[0],
        "vehicles": row[1],
        "min_time": row[2],
        "max_time": row[3],
        "avg_target_delay_delta": row[4],
        "delay_increase_rows": row[5],
    }
    print(
        f"{service_date}: {result['rows']:,} rows, "
        f"{result['vehicles']:,} vehicles, {result['bytes'] / 1024 / 1024:.1f} MiB",
    )
    return result


def write_manifest(manifest_path: Path, results: list[dict[str, Any]], args: argparse.Namespace) -> None:
    payload = {
        "source": str(resolve_path(args.lake_dir)),
        "output_dir": str(resolve_path(args.output_dir)),
        "sampling_seconds": args.sampling_seconds,
        "prediction_horizon_seconds": args.prediction_horizon_seconds,
        "target_tolerance_seconds": args.target_tolerance_seconds,
        "dates": results,
        "total_rows": sum(item["rows"] for item in results),
        "total_bytes": sum(item["bytes"] for item in results),
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    import duckdb

    args = parse_args()
    lake_dir = resolve_path(args.lake_dir)
    output_dir = resolve_path(args.output_dir)
    manifest_path = resolve_path(args.manifest)

    if not lake_dir.exists():
        raise FileNotFoundError(f"Lake directory does not exist: {lake_dir}")

    with duckdb.connect() as con:
        configure_duckdb(con, args.threads, args.memory_limit)
        all_dates = available_dates(con, lake_dir)
        if not all_dates:
            raise ValueError(f"No service_date partitions found in {lake_dir}")

        selected_dates = select_dates(args, all_dates)
        print(f"Available dates: {all_dates[0]} -> {all_dates[-1]} ({len(all_dates)} total)")
        print(f"Selected dates: {', '.join(selected_dates)}")

        results = [
            write_features_for_date(con, lake_dir, output_dir, service_date, args)
            for service_date in selected_dates
        ]

    write_manifest(manifest_path, results, args)
    print(f"Manifest: {manifest_path}")
    print(f"Total rows: {sum(item['rows'] for item in results):,}")


if __name__ == "__main__":
    main()
