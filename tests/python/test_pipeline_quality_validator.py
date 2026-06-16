from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ml.scripts.validate_pipeline_quality import ValidationConfig, validate_pipeline


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def create_valid_artifacts(
    tmp_path: Path,
    alert_time: datetime | None = None,
    lake_max_time: datetime | None = None,
) -> ValidationConfig:
    feature_path = tmp_path / "features" / "service_date=2026-06-13" / "part.parquet"
    feature_path.parent.mkdir(parents=True)
    feature_path.write_bytes(b"fake-parquet-placeholder")

    lake_manifest = tmp_path / "lake_manifest.json"
    feature_manifest = tmp_path / "feature_manifest.json"
    model_metadata = tmp_path / "model.joblib.json"
    alerts = tmp_path / "alerts.json"
    report = tmp_path / "reports" / "quality.json"
    scored_at = alert_time or datetime.now(UTC)
    lake_max = lake_max_time or datetime.now(UTC)

    write_json(
        lake_manifest,
        {
            "rows": 5000,
            "vehicles": 120,
            "parquet_files": 4,
            "partitions": 2,
            "min_time": (lake_max - timedelta(hours=3)).isoformat(),
            "max_time": lake_max.isoformat(),
        },
    )
    write_json(
        feature_manifest,
        {
            "total_rows": 2500,
            "total_bytes": 100,
            "dates": [
                {
                    "service_date": "2026-06-13",
                    "path": str(feature_path),
                    "rows": 2500,
                    "bytes": 100,
                }
            ],
        },
    )
    write_json(
        model_metadata,
        {
            "feature_columns": ["delay", "speed"],
            "precision_threshold": 0.75,
            "rows": {"train": 1000, "val": 500, "test": 500},
        },
    )
    write_json(
        alerts,
        [
            {
                "vehicle_id": "vehicle-1",
                "time": scored_at.isoformat(),
                "delay_increase_risk": 0.91,
                "delay_increase_alert": True,
            }
        ],
    )

    return ValidationConfig(
        lake_manifest=lake_manifest,
        feature_manifest=feature_manifest,
        model_metadata=model_metadata,
        alerts=alerts,
        report=report,
        min_lake_rows=1000,
        min_feature_rows=1000,
        max_alert_row_age_hours=48,
        check_feature_schema=False,
    )


def test_validate_pipeline_writes_passed_report(tmp_path: Path) -> None:
    config = create_valid_artifacts(tmp_path)

    report = validate_pipeline(config)

    assert report["status"] == "passed"
    assert report["summary"]["failed"] == 0
    assert config.report.exists()


def test_validate_pipeline_warns_on_zero_alerts_by_default(tmp_path: Path) -> None:
    config = create_valid_artifacts(tmp_path)
    write_json(config.alerts, [])

    report = validate_pipeline(config)

    assert report["status"] == "passed"
    assert report["summary"]["warnings"] >= 1
    assert any(
        check["name"] == "alert_artifact_rows" and check["status"] == "warning"
        for check in report["checks"]
    )


def test_validate_pipeline_can_fail_on_zero_alerts(tmp_path: Path) -> None:
    config = create_valid_artifacts(tmp_path)
    write_json(config.alerts, [])
    strict_config = ValidationConfig(
        **{
            **config.__dict__,
            "fail_on_zero_alerts": True,
        }
    )

    report = validate_pipeline(strict_config)

    assert report["status"] == "failed"
    assert any(
        check["name"] == "alert_artifact_rows" and check["status"] == "failed"
        for check in report["checks"]
    )


def test_validate_pipeline_fails_stale_alert_rows(tmp_path: Path) -> None:
    config = create_valid_artifacts(
        tmp_path,
        alert_time=datetime.now(UTC) - timedelta(days=3),
        lake_max_time=datetime.now(UTC),
    )

    report = validate_pipeline(config)

    assert report["status"] == "failed"
    assert any(
        check["name"] == "alert_artifact_freshness" and check["status"] == "failed"
        for check in report["checks"]
    )


def test_validate_pipeline_warns_when_source_data_is_stale(tmp_path: Path) -> None:
    stale_source_time = datetime.now(UTC) - timedelta(days=3)
    config = create_valid_artifacts(
        tmp_path,
        alert_time=stale_source_time - timedelta(minutes=5),
        lake_max_time=stale_source_time,
    )

    report = validate_pipeline(config)

    assert report["status"] == "passed"
    assert any(
        check["name"] == "lake_manifest_freshness" and check["status"] == "warning"
        for check in report["checks"]
    )
    assert any(
        check["name"] == "alert_artifact_freshness" and check["status"] == "warning"
        for check in report["checks"]
    )


def test_validate_pipeline_fails_missing_model_feature_columns(tmp_path: Path) -> None:
    config = create_valid_artifacts(tmp_path)
    write_json(config.model_metadata, {"rows": {"train": 1000, "val": 500, "test": 500}})

    report = validate_pipeline(config)

    assert report["status"] == "failed"
    assert any(
        check["name"] == "model_feature_columns" and check["status"] == "failed"
        for check in report["checks"]
    )
