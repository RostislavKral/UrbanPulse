from argparse import Namespace
from pathlib import Path

import pytest

from ml.scripts.build_delay_features_from_lake import feature_sql, parse_date_list, select_dates


def feature_args(**overrides: object) -> Namespace:
    values = {
        "dates": "",
        "start_date": "",
        "end_date": "",
        "latest_dates": 2,
        "all_dates": False,
    }
    values.update(overrides)
    return Namespace(**values)


def test_parse_date_list_normalizes_comma_separated_dates() -> None:
    assert parse_date_list(" 2026-05-19,2026-05-20 ") == [
        "2026-05-19",
        "2026-05-20",
    ]


def test_select_dates_prefers_explicit_dates() -> None:
    all_dates = ["2026-05-18", "2026-05-19", "2026-05-20"]

    selected = select_dates(
        feature_args(dates="2026-05-20,2026-05-18"),
        all_dates,
    )

    assert selected == ["2026-05-20", "2026-05-18"]


def test_select_dates_rejects_missing_explicit_dates() -> None:
    with pytest.raises(ValueError, match="2026-05-21"):
        select_dates(
            feature_args(dates="2026-05-21"),
            ["2026-05-19", "2026-05-20"],
        )


def test_select_dates_can_use_bounds_or_latest_dates() -> None:
    all_dates = [
        "2026-05-17",
        "2026-05-18",
        "2026-05-19",
        "2026-05-20",
    ]

    assert select_dates(
        feature_args(start_date="2026-05-18", end_date="2026-05-19"),
        all_dates,
    ) == ["2026-05-18", "2026-05-19"]
    assert select_dates(feature_args(), all_dates) == ["2026-05-19", "2026-05-20"]
    assert select_dates(feature_args(all_dates=True), all_dates) == all_dates


def test_feature_sql_keeps_unlabelled_rows_for_scoring(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")

    lake_dir = tmp_path / "lake"
    partition_dir = lake_dir / "service_date=2026-06-15"
    partition_dir.mkdir(parents=True)
    parquet_path = partition_dir / "part.parquet"

    with duckdb.connect() as con:
        con.execute("LOAD parquet")
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM (
                    VALUES
                    (
                        'vehicle-1',
                        TIMESTAMP '2026-06-15 08:00:00',
                        10,
                        35.0,
                        50.08,
                        14.42,
                        'bus',
                        '136',
                        'route-136',
                        'trip-1',
                        'IN_TRANSIT_TO',
                        3,
                        TIMESTAMP '2026-06-15 07:55:00',
                        'stop-1',
                        1,
                        TIMESTAMP '2026-06-15 07:59:00',
                        TIMESTAMP '2026-06-15 07:59:15',
                        'stop-2',
                        2,
                        TIMESTAMP '2026-06-15 08:02:00',
                        TIMESTAMP '2026-06-15 08:02:15'
                    ),
                    (
                        'vehicle-1',
                        TIMESTAMP '2026-06-15 08:00:30',
                        12,
                        33.0,
                        50.081,
                        14.421,
                        'bus',
                        '136',
                        'route-136',
                        'trip-1',
                        'IN_TRANSIT_TO',
                        3,
                        TIMESTAMP '2026-06-15 07:55:00',
                        'stop-1',
                        1,
                        TIMESTAMP '2026-06-15 07:59:00',
                        TIMESTAMP '2026-06-15 07:59:15',
                        'stop-2',
                        2,
                        TIMESTAMP '2026-06-15 08:02:00',
                        TIMESTAMP '2026-06-15 08:02:15'
                    )
                ) AS rows(
                    vehicle_id,
                    time,
                    delay,
                    speed,
                    lat,
                    lon,
                    mode,
                    line,
                    route_id,
                    trip_id,
                    state_position,
                    route_type,
                    origin_timestamp,
                    last_stop_id,
                    last_stop_sequence,
                    last_stop_arrival_time,
                    last_stop_departure_time,
                    next_stop_id,
                    next_stop_sequence,
                    next_stop_arrival_time,
                    next_stop_departure_time
                )
            )
            TO '{parquet_path}'
            (FORMAT PARQUET)
            """
        )

        frame = con.execute(
            feature_sql(
                lake_dir=lake_dir,
                service_date="2026-06-15",
                sampling_seconds=30,
                prediction_horizon_seconds=300,
                target_tolerance_seconds=600,
            )
        ).fetchdf()

    assert len(frame) == 1
    assert frame["vehicle_id"].iloc[0] == "vehicle-1"
    assert frame["target_delay"].isna().all()
    assert frame["target_delay_delta"].isna().all()
