from __future__ import annotations

import argparse
import json
import os
import shutil
from glob import glob
from pathlib import Path
from typing import Any

import duckdb

SCRIPT_PATH = Path(__file__).resolve()
ML_ROOT = SCRIPT_PATH.parents[1]
REPO_ROOT = SCRIPT_PATH.parents[2]

CSV_COLUMNS = {
    "time": "VARCHAR",
    "vehicle_id": "VARCHAR",
    "line": "VARCHAR",
    "delay": "VARCHAR",
    "lat": "VARCHAR",
    "lon": "VARCHAR",
    "speed": "VARCHAR",
    "route_id": "VARCHAR",
    "mode": "VARCHAR",
    "route_type": "VARCHAR",
    "trip_id": "VARCHAR",
    "state_position": "VARCHAR",
    "origin_timestamp": "VARCHAR",
    "last_stop_id": "VARCHAR",
    "last_stop_sequence": "VARCHAR",
    "last_stop_arrival_time": "VARCHAR",
    "last_stop_departure_time": "VARCHAR",
    "next_stop_id": "VARCHAR",
    "next_stop_sequence": "VARCHAR",
    "next_stop_arrival_time": "VARCHAR",
    "next_stop_departure_time": "VARCHAR",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a partitioned Parquet lake and DuckDB views from vehicle position CSV exports.",
    )
    parser.add_argument(
        "--input-glob",
        default="ml/vehicle_positions_*.csv.gz",
        help="Input CSV export glob.",
    )
    parser.add_argument(
        "--output-dir",
        default="ml/lake/vehicle_positions",
        help="Output directory for partitioned Parquet files.",
    )
    parser.add_argument(
        "--database",
        default="ml/lake/urbanpulse.duckdb",
        help="DuckDB database file for views and metadata.",
    )
    parser.add_argument(
        "--manifest",
        default="ml/lake/vehicle_positions_manifest.json",
        help="Manifest file written after a successful build.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Use only the first N input files. 0 means all files.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=max((os.cpu_count() or 2) - 1, 1),
        help="DuckDB worker threads.",
    )
    parser.add_argument(
        "--memory-limit",
        default="8GB",
        help="DuckDB memory limit.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete the output directory before writing.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Replace only service_date partitions present in the input files.",
    )
    parser.add_argument(
        "--views-only",
        action="store_true",
        help="Only recreate DuckDB views over an existing Parquet lake.",
    )
    return parser.parse_args()


def resolve_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def resolve_inputs(pattern: str, max_files: int) -> list[Path]:
    matches = [Path(path) for path in sorted(glob(str(resolve_path(pattern))))]
    if max_files > 0:
        matches = matches[:max_files]
    if not matches:
        raise FileNotFoundError(f"No input files matched {resolve_path(pattern)!s}")
    return matches


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_list(values: list[str]) -> str:
    return "[" + ", ".join(sql_string(value) for value in values) + "]"


def csv_columns_sql() -> str:
    entries = ", ".join(f"{key}: '{value}'" for key, value in CSV_COLUMNS.items())
    return "{" + entries + "}"


def typed_vehicle_positions_sql(input_paths: list[Path]) -> str:
    paths = sql_list([str(path) for path in input_paths])
    columns = csv_columns_sql()

    return f"""
        WITH raw AS (
            SELECT *
            FROM read_csv(
                {paths},
                header = true,
                columns = {columns},
                union_by_name = true,
                strict_mode = false,
                null_padding = true
            )
        ),
        typed AS (
            SELECT
                try_cast(time AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS time,
                vehicle_id,
                line,
                try_cast(delay AS INTEGER) AS delay,
                try_cast(lat AS DOUBLE) AS lat,
                try_cast(lon AS DOUBLE) AS lon,
                try_cast(speed AS DOUBLE) AS speed,
                route_id,
                mode,
                try_cast(route_type AS SMALLINT) AS route_type,
                trip_id,
                state_position,
                try_cast(origin_timestamp AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS origin_timestamp,
                last_stop_id,
                try_cast(last_stop_sequence AS INTEGER) AS last_stop_sequence,
                try_cast(last_stop_arrival_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC'
                    AS last_stop_arrival_time,
                try_cast(last_stop_departure_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC'
                    AS last_stop_departure_time,
                next_stop_id,
                try_cast(next_stop_sequence AS INTEGER) AS next_stop_sequence,
                try_cast(next_stop_arrival_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC'
                    AS next_stop_arrival_time,
                try_cast(next_stop_departure_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC'
                    AS next_stop_departure_time
            FROM raw
        )
        SELECT
            *,
            cast(time AS DATE) AS service_date
        FROM typed
        WHERE
            time IS NOT NULL
            AND vehicle_id IS NOT NULL
            AND lat IS NOT NULL
            AND lon IS NOT NULL
    """


def configure_duckdb(con: duckdb.DuckDBPyConnection, threads: int, memory_limit: str) -> None:
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = {sql_string(memory_limit)}")
    con.execute("INSTALL parquet")
    con.execute("LOAD parquet")


def write_lake(
    con: duckdb.DuckDBPyConnection,
    input_paths: list[Path],
    output_dir: Path,
    force: bool,
) -> None:
    if output_dir.exists():
        if not force:
            raise FileExistsError(
                f"{output_dir} already exists. Pass --force to rebuild it.",
            )
        shutil.rmtree(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    con.execute(
        f"""
        COPY (
            {typed_vehicle_positions_sql(input_paths)}
        )
        TO {sql_string(str(output_dir))}
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            PARTITION_BY (service_date)
        )
        """
    )


def write_lake_incremental(
    con: duckdb.DuckDBPyConnection,
    input_paths: list[Path],
    output_dir: Path,
) -> None:
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = output_dir.parent / f".{output_dir.name}_incremental_{os.getpid()}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    try:
        write_lake(con, input_paths, tmp_dir, force=True)
        partition_dirs = sorted(tmp_dir.glob("service_date=*"))
        if not partition_dirs:
            raise ValueError("Incremental lake build produced no service_date partitions.")

        for partition_dir in partition_dirs:
            target_dir = output_dir / partition_dir.name
            if target_dir.exists():
                shutil.rmtree(target_dir)
            shutil.move(str(partition_dir), str(target_dir))
            print(f"Replaced lake partition: {target_dir}")
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)


def create_views(con: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    parquet_glob = str(output_dir / "**" / "*.parquet")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW vehicle_positions AS
        SELECT *
        FROM read_parquet({sql_string(parquet_glob)}, hive_partitioning = true)
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW vehicle_positions_daily AS
        SELECT
            service_date,
            count(*) AS rows,
            count(DISTINCT vehicle_id) AS vehicles,
            min(time) AS min_time,
            max(time) AS max_time
        FROM vehicle_positions
        GROUP BY service_date
        ORDER BY service_date
        """
    )


def build_manifest(
    con: duckdb.DuckDBPyConnection,
    input_paths: list[Path],
    output_dir: Path,
) -> dict[str, Any]:
    stats = con.execute(
        """
        SELECT
            count(*) AS rows,
            count(DISTINCT vehicle_id) AS vehicles,
            cast(min(time) AS VARCHAR) AS min_time,
            cast(max(time) AS VARCHAR) AS max_time,
            count(DISTINCT service_date) AS partitions
        FROM vehicle_positions
        """
    ).fetchone()
    parquet_files = sorted(output_dir.glob("**/*.parquet"))

    return {
        "source": "vehicle_positions CSV exports",
        "input_files": [
            {
                "path": str(path),
                "bytes": path.stat().st_size,
            }
            for path in input_paths
        ],
        "output_dir": str(output_dir),
        "parquet_files": len(parquet_files),
        "parquet_bytes": sum(path.stat().st_size for path in parquet_files),
        "rows": stats[0],
        "vehicles": stats[1],
        "min_time": stats[2],
        "max_time": stats[3],
        "partitions": stats[4],
    }


def main() -> None:
    args = parse_args()
    input_paths = resolve_inputs(args.input_glob, args.max_files)
    output_dir = resolve_path(args.output_dir)
    database_path = resolve_path(args.database)
    manifest_path = resolve_path(args.manifest)

    print(f"Input files: {len(input_paths)}")
    for path in input_paths:
        print(f" - {path}")
    print(f"Output directory: {output_dir}")
    print(f"DuckDB database: {database_path}")

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as con:
        configure_duckdb(con, args.threads, args.memory_limit)
        if not args.views_only:
            if args.incremental:
                write_lake_incremental(con, input_paths, output_dir)
            else:
                write_lake(con, input_paths, output_dir, args.force)
        create_views(con, output_dir)
        manifest = build_manifest(con, input_paths, output_dir)

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print(f"Rows: {manifest['rows']:,}")
    print(f"Vehicles: {manifest['vehicles']:,}")
    print(f"Range: {manifest['min_time']} -> {manifest['max_time']}")
    print(f"Parquet files: {manifest['parquet_files']}")
    print(f"Parquet size: {manifest['parquet_bytes'] / 1024 / 1024:.1f} MiB")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
