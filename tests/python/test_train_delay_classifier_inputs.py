from pathlib import Path

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("joblib")
pytest.importorskip("sklearn")
train_module = pytest.importorskip("ml.scripts.train_delay_increase_classifier")


def write_feature_file(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        rows,
        schema={
            "time": pl.Datetime,
            "vehicle_id": pl.String,
            "delay": pl.Int64,
            "target_delay": pl.Int64,
        },
    ).write_parquet(path)


def test_filter_eligible_paths_skips_empty_target_partitions(tmp_path: Path) -> None:
    valid_path = tmp_path / "service_date=2026-06-14" / "part.parquet"
    empty_path = tmp_path / "service_date=2026-06-15" / "part.parquet"
    write_feature_file(
        valid_path,
        [
            {
                "time": "2026-06-14T12:00:00",
                "vehicle_id": "vehicle-1",
                "delay": 10,
                "target_delay": 30,
            }
        ],
    )
    write_feature_file(empty_path, [])

    filtered = train_module.filter_eligible_paths([str(valid_path), str(empty_path)], 60)

    assert filtered == [str(valid_path)]


def test_filter_train_window_keeps_recent_rows() -> None:
    frame = pl.DataFrame(
        {
            "time": [
                "2026-05-01T12:00:00",
                "2026-05-10T12:00:00",
                "2026-05-20T12:00:00",
            ],
            "target_increase": [0, 1, 0],
        },
        schema={"time": pl.Datetime, "target_increase": pl.Int8},
    )

    filtered, window = train_module.filter_train_window(
        frame,
        window_days=14,
        window_end="2026-05-20T12:00:00",
    )

    assert filtered["time"].dt.date().cast(pl.String).to_list() == [
        "2026-05-10",
        "2026-05-20",
    ]
    assert window.requested_days == 14
    assert window.rows_before == 3
    assert window.rows_after == 2
