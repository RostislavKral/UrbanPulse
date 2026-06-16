from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ml.scripts.evaluate_realtime_delay_predictions import (
    EvaluationConfig,
    build_report,
    calibration_bins,
    confusion_metrics,
    enrich_labeled_rows,
    policy_sweep,
)


def make_config(tmp_path: Path, *, fail_on_empty: bool = False) -> EvaluationConfig:
    return EvaluationConfig(
        report=tmp_path / "reports" / "realtime_eval.json",
        lookback_hours=24,
        horizon_minutes=5,
        match_tolerance_seconds=90,
        positive_delta_seconds=60,
        min_labeled_rows=3,
        max_predictions=1000,
        score_source="realtime",
        fail_on_empty=fail_on_empty,
    )


def test_confusion_metrics_for_map_alerts() -> None:
    rows = [
        {"delay_increase_alert": True, "actual_delay_increase": True},
        {"delay_increase_alert": True, "actual_delay_increase": False},
        {"delay_increase_alert": False, "actual_delay_increase": True},
        {"delay_increase_alert": False, "actual_delay_increase": False},
    ]

    metrics = confusion_metrics(rows, alert_column="delay_increase_alert")

    assert metrics["true_positive"] == 1
    assert metrics["false_positive"] == 1
    assert metrics["false_negative"] == 1
    assert metrics["true_negative"] == 1
    assert metrics["precision"] == 0.5
    assert metrics["recall"] == 0.5
    assert metrics["f1"] == 0.5


def test_enrich_labeled_rows_marks_missing_future_observations() -> None:
    rows = [
        {"actual_delay_delta": 75},
        {"actual_delay_delta": 12},
        {"actual_delay_delta": None},
    ]

    enriched = enrich_labeled_rows(rows, positive_delta_seconds=60)

    assert [row["is_labeled"] for row in enriched] == [True, True, False]
    assert [row["actual_delay_increase"] for row in enriched] == [True, False, None]


def test_calibration_bins_group_labeled_rows() -> None:
    rows = [
        {"delay_increase_risk": 0.15, "actual_delay_increase": False},
        {"delay_increase_risk": 0.19, "actual_delay_increase": True},
        {"delay_increase_risk": 0.82, "actual_delay_increase": True},
    ]

    bins = calibration_bins(rows, bucket_count=5)

    assert bins[0]["bucket"] == "0.0-0.2"
    assert bins[0]["count"] == 2
    assert bins[0]["positive_rate"] == 0.5
    assert bins[4]["bucket"] == "0.8-1.0"
    assert bins[4]["count"] == 1
    assert bins[4]["positive_rate"] == 1.0


def test_policy_sweep_simulates_min_risk_and_caps() -> None:
    rows = [
        {
            "scored_at": "2026-06-16T12:00:00+00:00",
            "delay_increase_risk": 0.91,
            "raw_delay_increase_alert": True,
            "actual_delay_increase": True,
        },
        {
            "scored_at": "2026-06-16T12:00:00+00:00",
            "delay_increase_risk": 0.82,
            "raw_delay_increase_alert": True,
            "actual_delay_increase": False,
        },
        {
            "scored_at": "2026-06-16T12:00:00+00:00",
            "delay_increase_risk": 0.72,
            "raw_delay_increase_alert": True,
            "actual_delay_increase": True,
        },
    ]

    sweep = policy_sweep(rows, min_risk_values=[0.0, 0.8], caps=[0, 1])

    assert sweep[0]["alerts"] == 3
    assert sweep[0]["precision"] == 2 / 3
    assert sweep[1]["alerts"] == 1
    assert sweep[1]["precision"] == 1.0
    assert sweep[2]["alerts"] == 2
    assert sweep[2]["precision"] == 0.5


def test_build_report_passes_when_enough_rows_are_labeled(tmp_path: Path) -> None:
    now = datetime.now(UTC).isoformat()
    rows = [
        {
            "scored_at": now,
            "vehicle_id": "v1",
            "position_time": now,
            "future_time": now,
            "delay": 100,
            "future_delay": 180,
            "actual_delay_delta": 80,
            "delay_increase_risk": 0.91,
            "delay_increase_alert": True,
            "raw_delay_increase_alert": True,
        },
        {
            "scored_at": now,
            "vehicle_id": "v2",
            "position_time": now,
            "future_time": now,
            "delay": 100,
            "future_delay": 120,
            "actual_delay_delta": 20,
            "delay_increase_risk": 0.75,
            "delay_increase_alert": True,
            "raw_delay_increase_alert": True,
        },
        {
            "scored_at": now,
            "vehicle_id": "v3",
            "position_time": now,
            "future_time": now,
            "delay": 100,
            "future_delay": 110,
            "actual_delay_delta": 10,
            "delay_increase_risk": 0.2,
            "delay_increase_alert": False,
            "raw_delay_increase_alert": False,
        },
    ]

    report = build_report(rows, make_config(tmp_path))

    assert report["status"] == "passed"
    assert report["summary"]["labeled_count"] == 3
    assert report["summary"]["positive_count"] == 1
    assert report["metrics"]["map_alerts"]["precision"] == 0.5
    assert report["policy_sweep"]


def test_build_report_warns_or_fails_on_empty_input(tmp_path: Path) -> None:
    warning_report = build_report([], make_config(tmp_path, fail_on_empty=False))
    failed_report = build_report([], make_config(tmp_path, fail_on_empty=True))

    assert warning_report["status"] == "warning"
    assert failed_report["status"] == "failed"
