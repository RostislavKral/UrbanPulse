from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

try:
    from .wandb_utils import init_wandb_run, log_file_artifact, log_metrics, log_table
except ImportError:
    from wandb_utils import init_wandb_run, log_file_artifact, log_metrics, log_table

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]


@dataclass(frozen=True)
class EvaluationConfig:
    report: Path
    lookback_hours: float
    horizon_minutes: float
    match_tolerance_seconds: int
    positive_delta_seconds: int
    min_labeled_rows: int
    max_predictions: int
    score_source: str
    fail_on_empty: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate saved realtime delay-increase predictions against later observed delays.",
    )
    parser.add_argument(
        "--report",
        default="ml/reports/realtime_delay_prediction_eval.json",
        help="Output JSON evaluation report.",
    )
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=24.0,
        help="Prediction scored_at lookback window.",
    )
    parser.add_argument(
        "--horizon-minutes",
        type=float,
        default=5.0,
        help="Future delay horizon used for labeling predictions.",
    )
    parser.add_argument(
        "--match-tolerance-seconds",
        type=int,
        default=90,
        help="Allowed timestamp distance around the target future horizon.",
    )
    parser.add_argument(
        "--positive-delta-seconds",
        type=int,
        default=60,
        help="Future delay increase threshold counted as a positive label.",
    )
    parser.add_argument(
        "--min-labeled-rows",
        type=int,
        default=30,
        help="Minimum labeled predictions needed for a passed report.",
    )
    parser.add_argument(
        "--max-predictions",
        type=int,
        default=20_000,
        help="Maximum saved predictions to evaluate.",
    )
    parser.add_argument(
        "--score-source",
        default="realtime",
        help="Optional score_source filter. Use an empty string for all sources.",
    )
    parser.add_argument(
        "--fail-on-empty",
        action="store_true",
        help="Exit with failure when no predictions can be evaluated.",
    )
    return parser.parse_args()


def resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def config_from_args(args: argparse.Namespace) -> EvaluationConfig:
    return EvaluationConfig(
        report=resolve_path(args.report),
        lookback_hours=args.lookback_hours,
        horizon_minutes=args.horizon_minutes,
        match_tolerance_seconds=args.match_tolerance_seconds,
        positive_delta_seconds=args.positive_delta_seconds,
        min_labeled_rows=args.min_labeled_rows,
        max_predictions=args.max_predictions,
        score_source=args.score_source,
        fail_on_empty=args.fail_on_empty,
    )


def load_root_env() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ.setdefault(key, value)


def get_db_url() -> str:
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        return explicit

    required_keys = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
    ]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    db = os.environ["POSTGRES_DB"]
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


EVALUATION_SQL = """
WITH bounds AS (
    SELECT max(time) AS latest_position_time
    FROM vehicle_positions
),
candidate_predictions AS (
    SELECT p.*, b.latest_position_time
    FROM delay_increase_predictions p
    CROSS JOIN bounds b
    WHERE
        p.scored_at >= now() - $1::interval
        AND p.position_time <= b.latest_position_time - $2::interval
        AND p.delay IS NOT NULL
        AND ($5::text = '' OR p.score_source = $5::text)
    ORDER BY p.scored_at DESC, p.delay_increase_risk DESC
    LIMIT $4
)
SELECT
    p.scored_at,
    p.vehicle_id,
    p.position_time,
    p.delay,
    p.delay_increase_risk,
    p.raw_delay_increase_alert,
    p.delay_increase_alert,
    p.alert_policy_reason,
    p.alert_rank,
    p.threshold,
    p.alert_min_risk,
    p.score_source,
    p.line,
    p.route_id,
    p.mode,
    future.time AS future_time,
    future.delay AS future_delay,
    future.delay - p.delay AS actual_delay_delta,
    ABS(EXTRACT(EPOCH FROM (future.time - (p.position_time + $2::interval))))
        AS target_lookup_lag_seconds,
    p.latest_position_time
FROM candidate_predictions p
LEFT JOIN LATERAL (
    SELECT v.time, v.delay
    FROM vehicle_positions v
    WHERE
        v.vehicle_id = p.vehicle_id
        AND v.delay IS NOT NULL
        AND v.time BETWEEN p.position_time + $2::interval - $3::interval
            AND p.position_time + $2::interval + $3::interval
    ORDER BY ABS(EXTRACT(EPOCH FROM (v.time - (p.position_time + $2::interval))))
    LIMIT 1
) future ON TRUE
ORDER BY p.scored_at DESC, p.delay_increase_risk DESC
"""


async def load_prediction_rows(config: EvaluationConfig) -> list[dict[str, Any]]:
    import asyncpg

    load_root_env()
    connection = await asyncpg.connect(get_db_url())
    try:
        rows = await connection.fetch(
            EVALUATION_SQL,
            timedelta(hours=config.lookback_hours),
            timedelta(minutes=config.horizon_minutes),
            timedelta(seconds=config.match_tolerance_seconds),
            config.max_predictions,
            config.score_source,
        )
    finally:
        await connection.close()

    return [dict(row) for row in rows]


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def safe_rate(numerator: int | float, denominator: int | float) -> float | None:
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def enrich_labeled_rows(
    rows: list[dict[str, Any]],
    *,
    positive_delta_seconds: int,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in rows:
        next_row = {key: json_safe(value) for key, value in row.items()}
        actual_delta = next_row.get("actual_delay_delta")
        next_row["is_labeled"] = isinstance(actual_delta, int | float)
        next_row["actual_delay_increase"] = (
            bool(actual_delta >= positive_delta_seconds)
            if isinstance(actual_delta, int | float)
            else None
        )
        enriched.append(next_row)
    return enriched


def confusion_metrics(
    rows: list[dict[str, Any]],
    *,
    alert_column: str,
) -> dict[str, Any]:
    labeled = [
        row
        for row in rows
        if isinstance(row.get("actual_delay_increase"), bool)
    ]
    true_positive = sum(
        1
        for row in labeled
        if bool(row.get(alert_column)) and bool(row.get("actual_delay_increase"))
    )
    false_positive = sum(
        1
        for row in labeled
        if bool(row.get(alert_column)) and not bool(row.get("actual_delay_increase"))
    )
    false_negative = sum(
        1
        for row in labeled
        if not bool(row.get(alert_column)) and bool(row.get("actual_delay_increase"))
    )
    true_negative = sum(
        1
        for row in labeled
        if not bool(row.get(alert_column)) and not bool(row.get("actual_delay_increase"))
    )

    predicted_positive = true_positive + false_positive
    actual_positive = true_positive + false_negative
    precision = safe_rate(true_positive, predicted_positive)
    recall = safe_rate(true_positive, actual_positive)
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and precision + recall > 0
        else None
    )

    return {
        "rows": len(labeled),
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "true_negative": true_negative,
        "predicted_positive": predicted_positive,
        "actual_positive": actual_positive,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "alert_rate": safe_rate(predicted_positive, len(labeled)),
        "positive_rate": safe_rate(actual_positive, len(labeled)),
    }


def calibration_bins(
    rows: list[dict[str, Any]],
    *,
    bucket_count: int = 10,
) -> list[dict[str, Any]]:
    labeled = [
        row
        for row in rows
        if isinstance(row.get("actual_delay_increase"), bool)
        and isinstance(row.get("delay_increase_risk"), int | float)
    ]
    buckets: list[dict[str, Any]] = []
    for index in range(bucket_count):
        lower = index / bucket_count
        upper = (index + 1) / bucket_count
        is_last = index == bucket_count - 1
        bucket_rows = [
            row
            for row in labeled
            if lower <= float(row["delay_increase_risk"]) <= upper
            if is_last or float(row["delay_increase_risk"]) < upper
        ]
        positives = sum(1 for row in bucket_rows if row["actual_delay_increase"])
        avg_risk = (
            sum(float(row["delay_increase_risk"]) for row in bucket_rows) / len(bucket_rows)
            if bucket_rows
            else None
        )
        buckets.append(
            {
                "bucket": f"{lower:.1f}-{upper:.1f}",
                "lower": lower,
                "upper": upper,
                "count": len(bucket_rows),
                "positive_count": positives,
                "positive_rate": safe_rate(positives, len(bucket_rows)),
                "avg_risk": avg_risk,
            }
        )
    return buckets


def policy_sweep(
    rows: list[dict[str, Any]],
    *,
    min_risk_values: list[float] | None = None,
    caps: list[int] | None = None,
) -> list[dict[str, Any]]:
    labeled = [
        row
        for row in rows
        if isinstance(row.get("actual_delay_increase"), bool)
    ]
    by_scored_at: dict[Any, list[dict[str, Any]]] = {}
    for row in labeled:
        by_scored_at.setdefault(row.get("scored_at"), []).append(row)

    min_risk_values = min_risk_values or [0.0, 0.75, 0.8, 0.85, 0.9]
    caps = caps or [0, 25, 15, 10]
    sweep_rows: list[dict[str, Any]] = []
    for min_risk in min_risk_values:
        for cap in caps:
            simulated_rows: list[dict[str, Any]] = []
            for group in by_scored_at.values():
                eligible = [
                    row
                    for row in group
                    if row.get("raw_delay_increase_alert")
                    and float(row.get("delay_increase_risk") or 0.0) >= min_risk
                ]
                eligible.sort(
                    key=lambda row: float(row.get("delay_increase_risk") or 0.0),
                    reverse=True,
                )
                selected = eligible if cap <= 0 else eligible[:cap]
                selected_ids = {id(row) for row in selected}
                for row in group:
                    simulated_rows.append(
                        {
                            **row,
                            "simulated_alert": id(row) in selected_ids,
                        }
                    )

            metrics = confusion_metrics(
                simulated_rows,
                alert_column="simulated_alert",
            )
            sweep_rows.append(
                {
                    "min_risk": min_risk,
                    "cap_per_run": cap,
                    "alerts": metrics["predicted_positive"],
                    "precision": metrics["precision"],
                    "recall": metrics["recall"],
                    "f1": metrics["f1"],
                    "alert_rate": metrics["alert_rate"],
                }
            )
    return sweep_rows


def compact_sample(rows: list[dict[str, Any]], *, limit: int = 12) -> list[dict[str, Any]]:
    sample_columns = [
        "scored_at",
        "vehicle_id",
        "position_time",
        "future_time",
        "delay_increase_risk",
        "delay_increase_alert",
        "raw_delay_increase_alert",
        "actual_delay_increase",
        "delay",
        "future_delay",
        "actual_delay_delta",
        "target_lookup_lag_seconds",
        "line",
        "route_id",
        "mode",
    ]
    return [
        {
            column: row.get(column)
            for column in sample_columns
            if column in row
        }
        for row in rows[:limit]
    ]


def report_status(
    *,
    prediction_count: int,
    labeled_count: int,
    config: EvaluationConfig,
) -> tuple[str, str]:
    if prediction_count == 0:
        status = "failed" if config.fail_on_empty else "warning"
        return status, "No saved realtime predictions were available for evaluation."
    if labeled_count == 0:
        status = "failed" if config.fail_on_empty else "warning"
        return status, "No predictions had a matching future observation yet."
    if labeled_count < config.min_labeled_rows:
        return (
            "warning",
            f"Only {labeled_count} labeled predictions were available. "
            f"The target minimum is {config.min_labeled_rows}.",
        )
    return "passed", "Realtime predictions were evaluated against future delay observations."


def build_report(
    rows: list[dict[str, Any]],
    config: EvaluationConfig,
) -> dict[str, Any]:
    enriched_rows = enrich_labeled_rows(
        rows,
        positive_delta_seconds=config.positive_delta_seconds,
    )
    labeled_rows = [row for row in enriched_rows if row["is_labeled"]]
    positives = sum(1 for row in labeled_rows if row["actual_delay_increase"])
    map_alerts = sum(1 for row in labeled_rows if row.get("delay_increase_alert"))
    raw_alerts = sum(1 for row in labeled_rows if row.get("raw_delay_increase_alert"))
    latest_prediction = max(
        (row.get("scored_at") for row in enriched_rows if row.get("scored_at")),
        default=None,
    )
    latest_position = max(
        (
            row.get("latest_position_time")
            for row in enriched_rows
            if row.get("latest_position_time")
        ),
        default=None,
    )
    status, reason = report_status(
        prediction_count=len(enriched_rows),
        labeled_count=len(labeled_rows),
        config=config,
    )

    labeled_sorted = sorted(
        labeled_rows,
        key=lambda row: float(row.get("delay_increase_risk") or 0.0),
        reverse=True,
    )

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": status,
        "reason": reason,
        "config": {
            **asdict(config),
            "report": str(config.report),
        },
        "summary": {
            "prediction_count": len(enriched_rows),
            "labeled_count": len(labeled_rows),
            "unlabeled_count": len(enriched_rows) - len(labeled_rows),
            "positive_count": positives,
            "map_alert_count": map_alerts,
            "raw_alert_count": raw_alerts,
            "latest_prediction_scored_at": json_safe(latest_prediction),
            "latest_position_time": json_safe(latest_position),
        },
        "metrics": {
            "map_alerts": confusion_metrics(enriched_rows, alert_column="delay_increase_alert"),
            "raw_threshold": confusion_metrics(
                enriched_rows,
                alert_column="raw_delay_increase_alert",
            ),
        },
        "calibration": calibration_bins(enriched_rows),
        "policy_sweep": policy_sweep(enriched_rows),
        "sample": compact_sample(labeled_sorted),
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def wandb_metrics_from_report(report: dict[str, Any]) -> dict[str, Any]:
    map_metrics = report["metrics"]["map_alerts"]
    raw_metrics = report["metrics"]["raw_threshold"]
    summary = report["summary"]
    return {
        "realtime_eval/predictions": summary["prediction_count"],
        "realtime_eval/labeled": summary["labeled_count"],
        "realtime_eval/positives": summary["positive_count"],
        "realtime_eval/map_alerts": summary["map_alert_count"],
        "realtime_eval/raw_alerts": summary["raw_alert_count"],
        "realtime_eval/map_precision": map_metrics["precision"],
        "realtime_eval/map_recall": map_metrics["recall"],
        "realtime_eval/map_f1": map_metrics["f1"],
        "realtime_eval/raw_precision": raw_metrics["precision"],
        "realtime_eval/raw_recall": raw_metrics["recall"],
        "realtime_eval/raw_f1": raw_metrics["f1"],
    }


async def run(config: EvaluationConfig) -> dict[str, Any]:
    rows = await load_prediction_rows(config)
    report = build_report(rows, config)
    write_report(config.report, report)
    return report


def main() -> None:
    config = config_from_args(parse_args())
    wandb_run = init_wandb_run(
        repo_root=REPO_ROOT,
        job_type="realtime-eval",
        tags=["urbanpulse", "delay-increase", "realtime-eval"],
        config={
            **asdict(config),
            "report": str(config.report),
        },
    )
    report = asyncio.run(run(config))
    print(f"Realtime prediction evaluation report: {config.report}")
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    print(json.dumps(report["metrics"], indent=2, sort_keys=True))

    log_metrics(wandb_run, wandb_metrics_from_report(report))
    log_table(wandb_run, "realtime_eval/calibration", report["calibration"])
    log_table(wandb_run, "realtime_eval/policy_sweep", report["policy_sweep"])
    log_file_artifact(
        wandb_run,
        config.report,
        name="realtime-delay-prediction-evaluation",
        artifact_type="evaluation",
        aliases=["latest"],
    )
    if wandb_run is not None:
        wandb_run.finish()

    if report["status"] == "failed":
        raise SystemExit(report["reason"])


if __name__ == "__main__":
    main()
