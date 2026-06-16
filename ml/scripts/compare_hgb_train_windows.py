from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run comparable HGB delay-risk training jobs over rolling windows.",
    )
    parser.add_argument(
        "--windows",
        default="14,30,60,90,180",
        help="Comma-separated rolling train windows in days.",
    )
    parser.add_argument(
        "--input-glob",
        default="ml/data/features/delay_5min_duckdb/service_date=*/part.parquet",
        help="Feature parquet glob passed to the trainer.",
    )
    parser.add_argument(
        "--model-output-dir",
        default="ml/models/window_backtests",
        help="Directory for per-window model artifacts.",
    )
    parser.add_argument(
        "--report",
        default="ml/reports/hgb_train_window_comparison.json",
        help="JSON report path.",
    )
    parser.add_argument("--max-rows", default="1000000")
    parser.add_argument("--selection", default="spread")
    parser.add_argument("--threshold-seconds", default="60")
    parser.add_argument("--min-precision", default="0.60")
    parser.add_argument("--alert-cooldown-minutes", default="15")
    parser.add_argument("--train-row-cap", default="300000")
    parser.add_argument("--holdout-last-files", default="1")
    parser.add_argument("--holdout-test-max-rows", default="300000")
    parser.add_argument("--learning-curve-rows", default="")
    parser.add_argument("--hgb-max-iter", default="200")
    parser.add_argument("--hgb-max-depth", default="6")
    parser.add_argument("--hgb-max-bins", default="127")
    parser.add_argument("--hgb-learning-rate", default="0.05")
    parser.add_argument(
        "--wandb-group",
        default="",
        help="W&B run group. Defaults to hgb-window-comparison-UTC_TIMESTAMP.",
    )
    parser.add_argument(
        "--min-train-rows",
        type=int,
        default=50_000,
        help="Minimum fit/train rows for automatic baseline recommendation.",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Continue after a failed window and include the failure in the report.",
    )
    parser.add_argument(
        "--reuse-existing",
        action="store_true",
        help="Reuse existing per-window metadata files instead of retraining.",
    )
    return parser.parse_args()


def resolve_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def parse_windows(value: str) -> list[int]:
    windows = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        days = int(part)
        if days <= 0:
            raise ValueError("Window days must be positive.")
        windows.append(days)
    if not windows:
        raise ValueError("At least one train window is required.")
    return sorted(set(windows))


def metric(metadata: dict[str, Any], key: str) -> float | int | None:
    value = metadata.get("metrics", {}).get(key)
    return value if isinstance(value, int | float) else None


def recommendation_score(result: dict[str, Any]) -> float:
    metadata = result.get("metadata")
    if not isinstance(metadata, dict):
        return -1.0

    high_conf_precision = metric(metadata, "test/high_confidence/precision")
    high_conf_recall = metric(metadata, "test/high_confidence/recall")
    top1_precision = metric(metadata, "test/top_1_percent_precision")
    average_precision = metric(metadata, "test/hgb_average_precision")
    event_alerts = metric(metadata, "test/high_confidence_event_alerts")

    score = 0.0
    if isinstance(high_conf_precision, int | float):
        score += float(high_conf_precision) * 0.40
    if isinstance(high_conf_recall, int | float):
        score += float(high_conf_recall) * 0.20
    if isinstance(top1_precision, int | float):
        score += float(top1_precision) * 0.20
    if isinstance(average_precision, int | float):
        score += float(average_precision) * 0.20
    if not isinstance(event_alerts, int | float) or event_alerts <= 0:
        score -= 0.20
    return score


def result_metric(result: dict[str, Any], key: str) -> float | int | None:
    metadata = result.get("metadata")
    if not isinstance(metadata, dict):
        return None
    return metric(metadata, key)


def choose_recommendation(
    results: list[dict[str, Any]],
    min_train_rows: int,
    min_precision: float,
) -> dict[str, Any] | None:
    candidates = []
    for result in results:
        if result.get("status") != "passed":
            continue
        metadata = result.get("metadata")
        if not isinstance(metadata, dict):
            continue
        fit_train_rows = int(metadata.get("rows", {}).get("fit_train") or 0)
        if fit_train_rows < min_train_rows:
            continue
        candidates.append((recommendation_score(result), result))

    if not candidates:
        return None

    precision_candidates = [
        (score, result)
        for score, result in candidates
        if (result_metric(result, "test/high_confidence/precision") or 0.0) >= min_precision
    ]
    if precision_candidates:
        candidates = precision_candidates

    best_score = max(score for score, _ in candidates)
    near_best = [
        result
        for score, result in candidates
        if score >= best_score * 0.98
    ]
    return min(near_best, key=lambda item: int(item["window_days"]))


def run_window(args: argparse.Namespace, days: int, group: str) -> dict[str, Any]:
    model_output_dir = resolve_path(args.model_output_dir)
    model_output = model_output_dir / f"delay_increase_hgb_{days}d.joblib"
    model_output_dir.mkdir(parents=True, exist_ok=True)
    trainer = REPO_ROOT / "ml" / "scripts" / "train_delay_increase_classifier.py"
    metadata_path = model_output.with_suffix(model_output.suffix + ".json")

    if args.reuse_existing:
        result: dict[str, Any] = {
            "window_days": days,
            "status": "passed" if metadata_path.exists() else "failed",
            "returncode": 0 if metadata_path.exists() else 1,
            "model_output": str(model_output),
            "metadata_path": str(metadata_path),
        }
        if metadata_path.exists():
            result["metadata"] = json.loads(metadata_path.read_text(encoding="utf-8"))
            result["recommendation_score"] = recommendation_score(result)
        print(f"Reused HGB rolling window metadata: {days}d")
        return result

    command = [
        sys.executable,
        str(trainer),
        "--input-glob",
        args.input_glob,
        "--max-rows",
        args.max_rows,
        "--selection",
        args.selection,
        "--threshold-seconds",
        args.threshold_seconds,
        "--min-precision",
        args.min_precision,
        "--alert-cooldown-minutes",
        args.alert_cooldown_minutes,
        "--train-row-cap",
        args.train_row_cap,
        "--holdout-last-files",
        args.holdout_last_files,
        "--holdout-test-max-rows",
        args.holdout_test_max_rows,
        "--train-window-days",
        str(days),
        "--learning-curve-rows",
        args.learning_curve_rows,
        "--hgb-max-iter",
        args.hgb_max_iter,
        "--hgb-max-depth",
        args.hgb_max_depth,
        "--hgb-max-bins",
        args.hgb_max_bins,
        "--hgb-learning-rate",
        args.hgb_learning_rate,
        "--model-output",
        str(model_output),
        "--artifact-aliases",
        f"comparison,hgb-window-{days}d",
    ]
    env = os.environ.copy()
    env["WANDB_RUN_GROUP"] = group
    env["WANDB_RUN_NAME"] = f"hgb_window_{days}d"

    print(f"\n=== HGB rolling window: {days}d ===")
    completed = subprocess.run(command, cwd=REPO_ROOT, env=env, check=False)
    result: dict[str, Any] = {
        "window_days": days,
        "status": "passed" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "model_output": str(model_output),
        "metadata_path": str(metadata_path),
    }

    if metadata_path.exists():
        result["metadata"] = json.loads(metadata_path.read_text(encoding="utf-8"))
        result["recommendation_score"] = recommendation_score(result)

    return result


def write_report(
    report_path: Path,
    group: str,
    results: list[dict[str, Any]],
    recommendation: dict[str, Any] | None,
    args: argparse.Namespace,
) -> None:
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "wandb_group": group,
        "windows": parse_windows(args.windows),
        "recommended_window_days": (
            recommendation.get("window_days") if recommendation else None
        ),
        "recommendation_reason": (
            "Highest composite score among windows with enough train rows and "
            "holdout high-confidence precision above the configured target; "
            "near ties prefer the shorter stable window. Windows below the "
            "precision target are only used when no successful window meets it."
            if recommendation
            else "No successful window met the minimum train-row requirement."
        ),
        "min_train_rows": args.min_train_rows,
        "min_precision": float(args.min_precision),
        "results": results,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nComparison report: {report_path}")
    if recommendation:
        print(f"Recommended baseline window: {recommendation['window_days']}d")
    else:
        print("Recommended baseline window: n/a")


def main() -> None:
    args = parse_args()
    windows = parse_windows(args.windows)
    group = args.wandb_group or (
        "hgb-window-comparison-"
        + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    results = []
    for days in windows:
        result = run_window(args, days, group)
        results.append(result)
        if result["status"] != "passed" and not args.keep_going:
            write_report(resolve_path(args.report), group, results, None, args)
            raise SystemExit(f"Window {days}d failed with return code {result['returncode']}.")

    recommendation = choose_recommendation(
        results,
        min_train_rows=args.min_train_rows,
        min_precision=float(args.min_precision),
    )
    write_report(resolve_path(args.report), group, results, recommendation, args)


if __name__ == "__main__":
    main()
