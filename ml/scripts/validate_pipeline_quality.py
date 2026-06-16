from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]

REQUIRED_ALERT_COLUMNS = {
    "vehicle_id",
    "time",
    "delay_increase_risk",
    "delay_increase_alert",
}
REQUIRED_FEATURE_CONTEXT_COLUMNS = {
    "vehicle_id",
    "time",
    "target_delay_delta",
}


@dataclass(frozen=True)
class ValidationConfig:
    lake_manifest: Path
    feature_manifest: Path
    model_metadata: Path
    alerts: Path
    report: Path
    min_lake_rows: int
    min_feature_rows: int
    max_alert_row_age_hours: float
    fail_on_zero_alerts: bool = False
    check_feature_schema: bool = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate UrbanPulse lake, feature, model, and alert artifacts.",
    )
    parser.add_argument(
        "--lake-manifest",
        default="ml/lake/vehicle_positions_manifest.json",
        help="Vehicle-position lake manifest.",
    )
    parser.add_argument(
        "--feature-manifest",
        default="ml/data/features/delay_5min_duckdb_manifest.json",
        help="Delay feature dataset manifest.",
    )
    parser.add_argument(
        "--model",
        default="ml/models/delay_increase_hgb_5min.joblib",
        help="Model artifact path. Used to derive MODEL.joblib.json when --model-metadata is omitted.",
    )
    parser.add_argument(
        "--model-metadata",
        default="",
        help="Explicit model metadata JSON path.",
    )
    parser.add_argument(
        "--alerts",
        default="ml/models/delay_increase_alerts.json",
        help="Scored alert JSON artifact.",
    )
    parser.add_argument(
        "--report",
        default="ml/reports/data_quality_latest.json",
        help="Output JSON quality report path.",
    )
    parser.add_argument(
        "--min-lake-rows",
        type=int,
        default=1000,
        help="Minimum acceptable rows in the lake manifest.",
    )
    parser.add_argument(
        "--min-feature-rows",
        type=int,
        default=1000,
        help="Minimum acceptable rows in the feature manifest.",
    )
    parser.add_argument(
        "--max-alert-row-age-hours",
        type=float,
        default=48.0,
        help="Maximum age of the latest scored alert row. Use <=0 to disable.",
    )
    parser.add_argument(
        "--fail-on-zero-alerts",
        action="store_true",
        help="Treat an empty alert artifact as a failure instead of a warning.",
    )
    parser.add_argument(
        "--skip-feature-schema-check",
        action="store_true",
        help="Skip DuckDB schema validation over feature parquet files.",
    )
    return parser.parse_args()


def resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def config_from_args(args: argparse.Namespace) -> ValidationConfig:
    model_metadata = (
        resolve_path(args.model_metadata)
        if args.model_metadata
        else resolve_path(args.model).with_suffix(resolve_path(args.model).suffix + ".json")
    )
    return ValidationConfig(
        lake_manifest=resolve_path(args.lake_manifest),
        feature_manifest=resolve_path(args.feature_manifest),
        model_metadata=model_metadata,
        alerts=resolve_path(args.alerts),
        report=resolve_path(args.report),
        min_lake_rows=args.min_lake_rows,
        min_feature_rows=args.min_feature_rows,
        max_alert_row_age_hours=args.max_alert_row_age_hours,
        fail_on_zero_alerts=args.fail_on_zero_alerts,
        check_feature_schema=not args.skip_feature_schema_check,
    )


def parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def add_check(
    checks: list[dict[str, Any]],
    name: str,
    status: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "name": name,
            "status": status,
            "message": message,
            "details": details or {},
        }
    )


def count_status(checks: list[dict[str, Any]], status: str) -> int:
    return sum(1 for check in checks if check["status"] == status)


def validate_lake_manifest(
    config: ValidationConfig,
    checks: list[dict[str, Any]],
) -> dict[str, Any] | None:
    path = config.lake_manifest
    if not path.exists():
        add_check(checks, "lake_manifest_exists", "failed", f"Missing lake manifest: {path}")
        return None

    try:
        manifest = load_json(path)
    except json.JSONDecodeError as exc:
        add_check(checks, "lake_manifest_valid_json", "failed", str(exc))
        return None

    if not isinstance(manifest, dict):
        add_check(checks, "lake_manifest_shape", "failed", "Lake manifest must be a JSON object.")
        return None

    rows = int(manifest.get("rows") or 0)
    parquet_files = int(manifest.get("parquet_files") or 0)
    vehicles = int(manifest.get("vehicles") or 0)
    partitions = int(manifest.get("partitions") or 0)
    max_time = parse_datetime(manifest.get("max_time"))

    add_check(
        checks,
        "lake_manifest_rows",
        "passed" if rows >= config.min_lake_rows else "failed",
        f"Lake rows: {rows:,}",
        {"rows": rows, "minimum": config.min_lake_rows},
    )
    add_check(
        checks,
        "lake_manifest_files",
        "passed" if parquet_files > 0 else "failed",
        f"Lake parquet files: {parquet_files}",
        {"parquet_files": parquet_files},
    )
    add_check(
        checks,
        "lake_manifest_vehicles",
        "passed" if vehicles > 0 else "failed",
        f"Lake vehicles: {vehicles:,}",
        {"vehicles": vehicles},
    )
    add_check(
        checks,
        "lake_manifest_partitions",
        "passed" if partitions > 0 else "failed",
        f"Lake service-date partitions: {partitions}",
        {"partitions": partitions},
    )
    add_check(
        checks,
        "lake_manifest_time_range",
        "passed" if max_time is not None else "failed",
        "Lake max_time is parseable." if max_time else "Lake max_time is missing or invalid.",
        {"min_time": manifest.get("min_time"), "max_time": manifest.get("max_time")},
    )
    if max_time is not None and config.max_alert_row_age_hours > 0:
        age = datetime.now(UTC) - max_time
        add_check(
            checks,
            "lake_manifest_freshness",
            "passed" if age <= timedelta(hours=config.max_alert_row_age_hours) else "warning",
            (
                f"Lake max_time age is {age.total_seconds() / 3600:.2f}h."
                if age <= timedelta(hours=config.max_alert_row_age_hours)
                else f"Lake/source data is stale: {age.total_seconds() / 3600:.2f}h."
            ),
            {
                "max_time": max_time.isoformat(),
                "age_hours": age.total_seconds() / 3600,
                "max_age_hours": config.max_alert_row_age_hours,
            },
        )
    return manifest


def feature_paths_from_manifest(manifest: dict[str, Any]) -> list[Path]:
    dates = manifest.get("dates")
    if not isinstance(dates, list):
        return []
    paths: list[Path] = []
    for item in dates:
        if not isinstance(item, dict):
            continue
        raw_path = item.get("path")
        if isinstance(raw_path, str) and raw_path:
            paths.append(resolve_path(raw_path))
    return paths


def validate_feature_manifest(
    config: ValidationConfig,
    checks: list[dict[str, Any]],
) -> dict[str, Any] | None:
    path = config.feature_manifest
    if not path.exists():
        add_check(
            checks,
            "feature_manifest_exists",
            "failed",
            f"Missing feature manifest: {path}",
        )
        return None

    try:
        manifest = load_json(path)
    except json.JSONDecodeError as exc:
        add_check(checks, "feature_manifest_valid_json", "failed", str(exc))
        return None

    if not isinstance(manifest, dict):
        add_check(
            checks,
            "feature_manifest_shape",
            "failed",
            "Feature manifest must be a JSON object.",
        )
        return None

    total_rows = int(manifest.get("total_rows") or 0)
    total_bytes = int(manifest.get("total_bytes") or 0)
    dates = manifest.get("dates") if isinstance(manifest.get("dates"), list) else []
    paths = feature_paths_from_manifest(manifest)
    missing_paths = [str(path) for path in paths if not path.exists()]

    add_check(
        checks,
        "feature_manifest_rows",
        "passed" if total_rows >= config.min_feature_rows else "failed",
        f"Feature rows: {total_rows:,}",
        {"rows": total_rows, "minimum": config.min_feature_rows},
    )
    add_check(
        checks,
        "feature_manifest_dates",
        "passed" if dates else "failed",
        f"Feature service dates: {len(dates)}",
        {"dates": [item.get("service_date") for item in dates if isinstance(item, dict)]},
    )
    add_check(
        checks,
        "feature_manifest_bytes",
        "passed" if total_bytes > 0 else "failed",
        f"Feature bytes: {total_bytes:,}",
        {"total_bytes": total_bytes},
    )
    add_check(
        checks,
        "feature_files_exist",
        "passed" if paths and not missing_paths else "failed",
        "All feature files exist." if paths and not missing_paths else "Some feature files are missing.",
        {"files": [str(path) for path in paths], "missing": missing_paths},
    )

    return manifest


def validate_model_metadata(
    config: ValidationConfig,
    checks: list[dict[str, Any]],
) -> dict[str, Any] | None:
    path = config.model_metadata
    if not path.exists():
        add_check(
            checks,
            "model_metadata_exists",
            "failed",
            f"Missing model metadata: {path}",
        )
        return None

    try:
        metadata = load_json(path)
    except json.JSONDecodeError as exc:
        add_check(checks, "model_metadata_valid_json", "failed", str(exc))
        return None

    if not isinstance(metadata, dict):
        add_check(
            checks,
            "model_metadata_shape",
            "failed",
            "Model metadata must be a JSON object.",
        )
        return None

    feature_columns = metadata.get("feature_columns")
    rows = metadata.get("rows")
    precision_threshold = metadata.get("precision_threshold")
    row_details = rows if isinstance(rows, dict) else {}
    rows_are_positive = all(int(row_details.get(key) or 0) > 0 for key in ["train", "val", "test"])

    add_check(
        checks,
        "model_feature_columns",
        "passed" if isinstance(feature_columns, list) and len(feature_columns) > 0 else "failed",
        f"Model feature columns: {len(feature_columns) if isinstance(feature_columns, list) else 0}",
        {"feature_columns": feature_columns if isinstance(feature_columns, list) else []},
    )
    add_check(
        checks,
        "model_training_rows",
        "passed" if rows_are_positive else "failed",
        "Model metadata has train/val/test row counts.",
        {"rows": row_details},
    )
    add_check(
        checks,
        "model_precision_threshold",
        "passed" if isinstance(precision_threshold, int | float) else "warning",
        (
            f"Precision threshold: {precision_threshold}"
            if isinstance(precision_threshold, int | float)
            else "Precision threshold is missing; scorer will fall back to 0.5."
        ),
        {"precision_threshold": precision_threshold},
    )

    return metadata


def read_feature_schema(paths: list[Path]) -> set[str]:
    import duckdb

    quoted_paths = ", ".join("'" + str(path).replace("'", "''") + "'" for path in paths)
    with duckdb.connect() as con:
        rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet([{quoted_paths}]) LIMIT 0"
        ).fetchall()
    return {str(row[0]) for row in rows}


def validate_feature_schema(
    feature_manifest: dict[str, Any] | None,
    model_metadata: dict[str, Any] | None,
    config: ValidationConfig,
    checks: list[dict[str, Any]],
) -> None:
    if not config.check_feature_schema:
        add_check(
            checks,
            "feature_schema",
            "warning",
            "Feature parquet schema check was skipped.",
        )
        return
    if feature_manifest is None or model_metadata is None:
        return

    feature_columns = model_metadata.get("feature_columns")
    if not isinstance(feature_columns, list) or not feature_columns:
        return

    paths = [path for path in feature_paths_from_manifest(feature_manifest) if path.exists()]
    if not paths:
        return

    try:
        schema = read_feature_schema(paths[: min(len(paths), 5)])
    except Exception as exc:
        add_check(
            checks,
            "feature_schema",
            "failed",
            f"Failed to read feature parquet schema: {exc}",
        )
        return

    required_columns = set(feature_columns) | REQUIRED_FEATURE_CONTEXT_COLUMNS
    missing_columns = sorted(required_columns - schema)
    add_check(
        checks,
        "feature_schema",
        "passed" if not missing_columns else "failed",
        "Feature parquet schema includes model and scorer columns."
        if not missing_columns
        else "Feature parquet schema is missing required columns.",
        {"missing_columns": missing_columns, "checked_files": [str(path) for path in paths[:5]]},
    )


def validate_alert_artifact(
    config: ValidationConfig,
    checks: list[dict[str, Any]],
    lake_manifest: dict[str, Any] | None,
) -> None:
    path = config.alerts
    if not path.exists():
        add_check(checks, "alert_artifact_exists", "failed", f"Missing alert artifact: {path}")
        return

    try:
        records = load_json(path)
    except json.JSONDecodeError as exc:
        add_check(checks, "alert_artifact_valid_json", "failed", str(exc))
        return

    if not isinstance(records, list):
        add_check(
            checks,
            "alert_artifact_shape",
            "failed",
            "Alert artifact must be a JSON array.",
        )
        return

    add_check(
        checks,
        "alert_artifact_rows",
        "failed" if config.fail_on_zero_alerts and len(records) == 0 else "warning"
        if len(records) == 0
        else "passed",
        f"Alert rows: {len(records):,}",
        {"rows": len(records), "fail_on_zero_alerts": config.fail_on_zero_alerts},
    )
    if not records:
        return

    invalid_records = [
        index
        for index, record in enumerate(records)
        if not isinstance(record, dict) or not REQUIRED_ALERT_COLUMNS.issubset(record.keys())
    ]
    add_check(
        checks,
        "alert_artifact_required_columns",
        "passed" if not invalid_records else "failed",
        "Alert rows include required columns."
        if not invalid_records
        else "Some alert rows are missing required columns.",
        {"invalid_record_indexes": invalid_records[:20]},
    )

    parsed_times = [
        parsed
        for parsed in (parse_datetime(record.get("time")) for record in records if isinstance(record, dict))
        if parsed is not None
    ]
    latest_time = max(parsed_times) if parsed_times else None
    if latest_time is None:
        add_check(
            checks,
            "alert_artifact_latest_time",
            "failed",
            "Alert rows do not include a parseable time.",
        )
        return

    if config.max_alert_row_age_hours <= 0:
        add_check(
            checks,
            "alert_artifact_freshness",
            "warning",
            "Alert row freshness check is disabled.",
            {"latest_time": latest_time.isoformat()},
        )
        return

    max_age = timedelta(hours=config.max_alert_row_age_hours)
    age = datetime.now(UTC) - latest_time
    lake_max_time = (
        parse_datetime(lake_manifest.get("max_time"))
        if isinstance(lake_manifest, dict)
        else None
    )
    lake_age = datetime.now(UTC) - lake_max_time if lake_max_time is not None else None
    alert_lag_from_lake = (
        lake_max_time - latest_time if lake_max_time is not None else None
    )
    if (
        age > max_age
        and lake_age is not None
        and lake_age > max_age
        and alert_lag_from_lake is not None
        and alert_lag_from_lake <= max_age
    ):
        add_check(
            checks,
            "alert_artifact_freshness",
            "warning",
            "Latest alert row is stale because the lake/source data is stale.",
            {
                "latest_time": latest_time.isoformat(),
                "age_hours": age.total_seconds() / 3600,
                "lake_max_time": lake_max_time.isoformat(),
                "lake_age_hours": lake_age.total_seconds() / 3600,
                "alert_lag_from_lake_hours": alert_lag_from_lake.total_seconds() / 3600,
                "max_age_hours": config.max_alert_row_age_hours,
            },
        )
        return

    add_check(
        checks,
        "alert_artifact_freshness",
        "passed" if age <= max_age else "failed",
        (
            f"Latest alert row age is {age.total_seconds() / 3600:.2f}h."
            if age <= max_age
            else f"Latest alert row is too old: {age.total_seconds() / 3600:.2f}h."
        ),
        {
            "latest_time": latest_time.isoformat(),
            "age_hours": age.total_seconds() / 3600,
            "max_age_hours": config.max_alert_row_age_hours,
        },
    )


def build_report(config: ValidationConfig, checks: list[dict[str, Any]]) -> dict[str, Any]:
    failed = count_status(checks, "failed")
    warnings = count_status(checks, "warning")
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "failed" if failed else "passed",
        "summary": {
            "failed": failed,
            "warnings": warnings,
            "passed": count_status(checks, "passed"),
        },
        "artifacts": {
            "lake_manifest": str(config.lake_manifest),
            "feature_manifest": str(config.feature_manifest),
            "model_metadata": str(config.model_metadata),
            "alerts": str(config.alerts),
            "report": str(config.report),
        },
        "checks": checks,
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_pipeline(config: ValidationConfig) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    lake_manifest = validate_lake_manifest(config, checks)
    feature_manifest = validate_feature_manifest(config, checks)
    model_metadata = validate_model_metadata(config, checks)
    validate_feature_schema(feature_manifest, model_metadata, config, checks)
    validate_alert_artifact(config, checks, lake_manifest)
    report = build_report(config, checks)
    write_report(config.report, report)
    return report


def main() -> None:
    config = config_from_args(parse_args())
    report = validate_pipeline(config)

    print(f"Pipeline quality report: {config.report}")
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    if report["status"] != "passed":
        failed_messages = [
            f"- {check['name']}: {check['message']}"
            for check in report["checks"]
            if check["status"] == "failed"
        ]
        raise SystemExit("Pipeline quality validation failed:\n" + "\n".join(failed_messages))


if __name__ == "__main__":
    main()
