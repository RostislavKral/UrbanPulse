from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType


def load_compare_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[2] / "ml/scripts/compare_hgb_train_windows.py"
    spec = importlib.util.spec_from_file_location("compare_hgb_train_windows", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def comparison_result(
    window_days: int,
    *,
    high_confidence_precision: float,
    high_confidence_recall: float,
    average_precision: float,
) -> dict:
    return {
        "window_days": window_days,
        "status": "passed",
        "metadata": {
            "rows": {"fit_train": 300_000},
            "metrics": {
                "test/high_confidence/precision": high_confidence_precision,
                "test/high_confidence/recall": high_confidence_recall,
                "test/top_1_percent_precision": 0.85,
                "test/hgb_average_precision": average_precision,
                "test/high_confidence_event_alerts": 500,
            },
        },
    }


def test_recommendation_prefers_precision_target_before_score() -> None:
    compare_module = load_compare_module()

    recommendation = compare_module.choose_recommendation(
        [
            comparison_result(
                14,
                high_confidence_precision=0.59,
                high_confidence_recall=0.23,
                average_precision=0.48,
            ),
            comparison_result(
                30,
                high_confidence_precision=0.61,
                high_confidence_recall=0.18,
                average_precision=0.46,
            ),
        ],
        min_train_rows=50_000,
        min_precision=0.60,
    )

    assert recommendation["window_days"] == 30
