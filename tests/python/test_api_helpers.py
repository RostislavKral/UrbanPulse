import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import main
import pytest


@pytest.fixture(autouse=True)
def clear_realtime_alert_state(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(main, "REALTIME_ALERT_SNAPSHOT", None)
    monkeypatch.setattr(main, "REALTIME_ALERT_ERROR", None)
    yield


def test_health_endpoint_payload() -> None:
    assert asyncio.run(main.health()) == {"status": "ok"}


def test_coerce_alert_record_normalizes_risk_and_alert_flag() -> None:
    record = {
        "vehicle_id": "vehicle-1",
        "delay_increase_risk": "0.75",
        "delay_increase_alert": 1,
    }

    coerced = main._coerce_alert_record(record)

    assert coerced is not None
    assert coerced["delay_increase_risk"] == 0.75
    assert coerced["delay_increase_alert"] is True


def test_coerce_alert_record_defaults_invalid_risk() -> None:
    coerced = main._coerce_alert_record(
        {
            "vehicle_id": "vehicle-1",
            "delay_increase_risk": "not-a-number",
            "delay_increase_alert": False,
        }
    )

    assert coerced is not None
    assert coerced["delay_increase_risk"] == 0.0
    assert coerced["delay_increase_alert"] is False


def test_coerce_alert_record_rejects_non_mapping_values() -> None:
    assert main._coerce_alert_record(["not", "a", "record"]) is None


def test_delay_alert_path_defaults_to_local_artifact_for_local_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DELAY_ALERTS_PATH", raising=False)

    assert main._get_delay_alerts_path() == main.LOCAL_DELAY_ALERTS_PATH


def test_delay_alert_endpoint_returns_empty_payload_when_artifact_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "missing_alerts.json"
    monkeypatch.setenv("DELAY_ALERTS_PATH", str(missing_path))

    response = asyncio.run(main.delay_increase_alerts(limit=12, alerts_only=True))

    assert response["data"] == []
    assert response["meta"]["artifact_path"] == str(missing_path)
    assert response["meta"]["artifact_exists"] is False
    assert response["meta"]["returned_count"] == 0
    assert response["meta"]["status"] == "missing"
    assert response["meta"]["scored_count"] == 0
    assert response["meta"]["alert_count"] == 0


def test_delay_alert_endpoint_reports_scored_and_threshold_alert_counts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    alerts_path = tmp_path / "alerts.json"
    alerts_path.write_text(
        json.dumps(
            [
                {
                    "vehicle_id": "v1",
                    "time": now.isoformat(),
                    "delay_increase_risk": 0.91,
                    "delay_increase_alert": True,
                },
                {
                    "vehicle_id": "v2",
                    "time": now.isoformat(),
                    "delay_increase_risk": 0.42,
                    "delay_increase_alert": False,
                },
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DELAY_ALERTS_PATH", str(alerts_path))

    response = asyncio.run(main.delay_increase_alerts(limit=12, alerts_only=False))

    assert response["meta"]["total_count"] == 2
    assert response["meta"]["scored_count"] == 2
    assert response["meta"]["alert_count"] == 1
    assert response["meta"]["returned_count"] == 2


def test_delay_alert_endpoint_prefers_realtime_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_path = tmp_path / "alerts.json"
    artifact_path.write_text(
        '[{"vehicle_id":"artifact","delay_increase_risk":0.1,"delay_increase_alert":false}]',
        encoding="utf-8",
    )
    monkeypatch.setenv("DELAY_ALERTS_PATH", str(artifact_path))
    now = datetime.now(UTC)
    monkeypatch.setattr(
        main,
        "REALTIME_ALERT_SNAPSHOT",
        {
            "generated_at": now.isoformat(),
            "model_path": "/models/model.joblib",
            "threshold": 0.6,
            "context_minutes": 20,
            "sampling_seconds": 30,
            "output_freshness_seconds": 180,
            "alert_max_per_run": 2,
            "alert_min_risk": 0.4,
            "raw_alert_count": 1,
            "eligible_alert_count": 1,
            "suppressed_alert_count": 0,
            "persisted_prediction_count": 2,
            "records": [
                {
                    "vehicle_id": "low-risk",
                    "time": now.isoformat(),
                    "delay_increase_risk": 0.2,
                    "delay_increase_alert": False,
                },
                {
                    "vehicle_id": "high-risk",
                    "time": now.isoformat(),
                    "delay_increase_risk": 0.9,
                    "delay_increase_alert": True,
                },
            ],
        },
    )

    response = asyncio.run(main.delay_increase_alerts(limit=10, alerts_only=False))

    assert response["meta"]["source"] == "realtime"
    assert response["meta"]["artifact_path"] == "realtime://vehicle_positions"
    assert response["meta"]["scored_count"] == 2
    assert response["meta"]["alert_count"] == 1
    assert response["meta"]["threshold"] == 0.6
    assert response["meta"]["alert_max_per_run"] == 2
    assert response["meta"]["raw_alert_count"] == 1
    assert response["meta"]["persisted_prediction_count"] == 2
    assert [record["vehicle_id"] for record in response["data"]] == [
        "high-risk",
        "low-risk",
    ]


def test_delay_alert_endpoint_falls_back_to_artifact_when_realtime_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    artifact_path = tmp_path / "alerts.json"
    artifact_path.write_text(
        json.dumps(
            [
                {
                    "vehicle_id": "artifact",
                    "time": now.isoformat(),
                    "delay_increase_risk": 0.7,
                    "delay_increase_alert": True,
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DELAY_ALERTS_PATH", str(artifact_path))
    monkeypatch.setattr(main, "REALTIME_ALERT_ERROR", "model missing")

    response = asyncio.run(main.delay_increase_alerts(limit=10, alerts_only=False))

    assert response["meta"]["source"] == "artifact"
    assert response["meta"]["scored_count"] == 1
    assert response["data"][0]["vehicle_id"] == "artifact"


def test_delay_alert_quality_uses_realtime_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    monkeypatch.setattr(
        main,
        "REALTIME_ALERT_SNAPSHOT",
        {
            "generated_at": now.isoformat(),
            "model_path": "/models/model.joblib",
            "threshold": 0.6,
            "context_minutes": 20,
            "sampling_seconds": 30,
            "records": [
                {
                    "vehicle_id": "vehicle-1",
                    "time": now.isoformat(),
                    "delay_increase_risk": 0.9,
                    "delay_increase_alert": True,
                }
            ],
        },
    )

    quality = main._delay_alert_quality(now)

    assert quality["status"] == "fresh"
    assert quality["source"] == "realtime"
    assert quality["total_count"] == 1
    assert quality["alert_count"] == 1


def test_realtime_alert_policy_caps_alerts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main, "REALTIME_ALERT_MAX_PER_RUN", 2)
    monkeypatch.setattr(main, "REALTIME_ALERT_MIN_RISK", 0.0)
    records = [
        {"vehicle_id": "v1", "delay_increase_risk": 0.91},
        {"vehicle_id": "v2", "delay_increase_risk": 0.82},
        {"vehicle_id": "v3", "delay_increase_risk": 0.73},
        {"vehicle_id": "v4", "delay_increase_risk": 0.44},
    ]

    policy = main._apply_realtime_alert_policy(records, threshold=0.7)

    assert policy["raw_alert_count"] == 3
    assert policy["eligible_alert_count"] == 3
    assert policy["suppressed_alert_count"] == 1
    assert [record["delay_increase_alert"] for record in records] == [
        True,
        True,
        False,
        False,
    ]
    assert records[0]["alert_rank"] == 1
    assert records[1]["alert_rank"] == 2
    assert records[2]["alert_policy_reason"] == "suppressed_by_cap"
    assert records[3]["alert_policy_reason"] == "below_threshold"


def test_realtime_alert_policy_applies_minimum_risk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main, "REALTIME_ALERT_MAX_PER_RUN", 10)
    monkeypatch.setattr(main, "REALTIME_ALERT_MIN_RISK", 0.8)
    records = [
        {"vehicle_id": "v1", "delay_increase_risk": 0.9},
        {"vehicle_id": "v2", "delay_increase_risk": 0.75},
    ]

    policy = main._apply_realtime_alert_policy(records, threshold=0.7)

    assert policy["raw_alert_count"] == 2
    assert policy["eligible_alert_count"] == 1
    assert records[0]["delay_increase_alert"] is True
    assert records[1]["delay_increase_alert"] is False
    assert records[1]["alert_policy_reason"] == "below_min_risk"


def test_prediction_insert_tuple_normalizes_realtime_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main, "REALTIME_ALERT_MIN_RISK", 0.4)
    now = datetime.now(UTC)
    record = {
        "scored_at": now.isoformat(),
        "time": now.isoformat(),
        "vehicle_id": "vehicle-1",
        "delay_increase_risk": 0.87,
        "raw_delay_increase_alert": True,
        "delay_increase_alert": True,
        "alert_policy_reason": "selected",
        "alert_rank": 1,
        "score_source": "realtime",
        "line": "5",
        "route_id": "route-5",
        "delay": 120,
        "speed": 18,
        "lat": 50.08,
        "lon": 14.43,
    }
    artifact = {"threshold": 0.6, "path": "/models/model.joblib", "mtime": 123.4}

    values = main._prediction_insert_tuple(record, artifact)

    assert values is not None
    by_column = dict(zip(main.PREDICTION_INSERT_COLUMNS, values, strict=True))
    assert by_column["vehicle_id"] == "vehicle-1"
    assert by_column["position_time"] == now
    assert by_column["delay_increase_risk"] == 0.87
    assert by_column["threshold"] == 0.6
    assert by_column["alert_min_risk"] == 0.4
    assert by_column["model_path"] == "/models/model.joblib"


def test_source_status_reports_fresh_stale_and_missing() -> None:
    now = datetime(2026, 6, 13, 12, 0, tzinfo=UTC)

    assert main._source_status(now, 30, 120, rows_near_latest=10)[0] == "fresh"
    assert main._source_status(now - timedelta(minutes=5), 300, 120, 10)[0] == "stale"
    assert main._source_status(None, None, 120, 0)[0] == "missing"
    assert main._source_status(now, 30, 120, rows_near_latest=0)[0] == "missing"


def test_overall_quality_status_uses_worst_source_status() -> None:
    assert main._overall_quality_status(["fresh", "fresh"]) == "fresh"
    assert main._overall_quality_status(["fresh", "stale"]) == "stale"
    assert main._overall_quality_status(["fresh", "missing"]) == "missing"
    assert main._overall_quality_status(["fresh", "error"]) == "error"


def test_delay_alert_quality_reports_fresh_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    alerts_path = tmp_path / "alerts.json"
    alerts_path.write_text(
        '[{"vehicle_id":"v1","time":"'
        + now.isoformat()
        + '","delay_increase_risk":0.9,"delay_increase_alert":true}]',
        encoding="utf-8",
    )
    monkeypatch.setenv("DELAY_ALERTS_PATH", str(alerts_path))

    quality = main._delay_alert_quality(now)

    assert quality["status"] == "fresh"
    assert quality["artifact_exists"] is True
    assert quality["total_count"] == 1
    assert quality["alert_count"] == 1
    assert quality["latest_alert_time"] == now.isoformat()


def test_delay_alert_quality_reports_stale_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    old_time = now - timedelta(seconds=main.ALERT_ROW_FRESHNESS_SECONDS + 10)
    alerts_path = tmp_path / "alerts.json"
    alerts_path.write_text(
        '[{"vehicle_id":"v1","time":"'
        + old_time.isoformat()
        + '","delay_increase_risk":0.9,"delay_increase_alert":true}]',
        encoding="utf-8",
    )
    monkeypatch.setenv("DELAY_ALERTS_PATH", str(alerts_path))

    quality = main._delay_alert_quality(now)

    assert quality["status"] == "stale"
    assert "Latest scored row" in quality["reason"]


def test_pipeline_report_quality_reports_fresh_passed_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    report_path = tmp_path / "data_quality_latest.json"
    report_path.write_text(
        json.dumps(
            {
                "generated_at": now.isoformat(),
                "status": "passed",
                "summary": {"failed": 0, "warnings": 0, "passed": 17},
                "checks": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PIPELINE_QUALITY_REPORT_PATH", str(report_path))

    quality = main._pipeline_report_quality(now)

    assert quality["status"] == "fresh"
    assert quality["report_exists"] is True
    assert quality["summary"]["passed"] == 17
    assert quality["checks"] == []


def test_pipeline_report_quality_surfaces_failed_checks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    report_path = tmp_path / "data_quality_latest.json"
    report_path.write_text(
        json.dumps(
            {
                "generated_at": now.isoformat(),
                "status": "failed",
                "summary": {"failed": 1, "warnings": 0, "passed": 16},
                "checks": [
                    {
                        "name": "alert_artifact_freshness",
                        "status": "failed",
                        "message": "Latest alert row is too old.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PIPELINE_QUALITY_REPORT_PATH", str(report_path))

    quality = main._pipeline_report_quality(now)

    assert quality["status"] == "error"
    assert quality["summary"]["failed"] == 1
    assert quality["checks"] == [
        {
            "name": "alert_artifact_freshness",
            "status": "failed",
            "message": "Latest alert row is too old.",
        }
    ]
