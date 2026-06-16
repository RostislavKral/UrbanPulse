import ast
from pathlib import Path

DAG_PATH = Path("airflow/dags/urbanpulse_ml_alerts.py")
FETCH_SCRIPT_PATH = Path("ml/scripts/fetch_vehicle_exports.sh")


def test_airflow_dag_file_parses() -> None:
    ast.parse(DAG_PATH.read_text(encoding="utf-8"))


def test_airflow_dag_defines_expected_pipeline_tasks() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")

    for task_id in [
        "fetch_vehicle_exports",
        "migrate_operational_db",
        "build_vehicle_positions_lake",
        "build_delay_features",
        "compare_hgb_train_windows",
        "train_delay_increase_model",
        "score_delay_alerts",
        "validate_pipeline_quality",
        "validate_scored_alerts",
        "evaluate_realtime_predictions",
    ]:
        assert f'task_id="{task_id}"' in source

    assert "fetch_vehicle_exports" in source
    assert "BUILD_LAKE_FROM_CSV = FETCH_EXPORTS or REBUILD_LAKE" in source
    assert "ml/data/raw/vehicle_exports/vehicle_positions_*.csv.gz" in source
    assert "--incremental" in source
    assert "--learning-curve-rows" in source
    assert "--train-window-days" in source
    assert "compare_hgb_train_windows" in source
    assert "URBANPULSE_RUN_WINDOW_COMPARISON=false" in source
    assert ">> migrate_operational_db" in source
    assert ">> build_vehicle_positions_lake" in source
    assert ">> build_delay_features" in source
    assert ">> compare_hgb_train_windows" in source
    assert ">> train_delay_increase_model" in source
    assert ">> score_delay_alerts" in source
    assert ">> validate_pipeline_quality" in source
    assert ">> validate_scored_alerts" in source
    assert ">> evaluate_realtime_predictions" in source
    assert "ml/scripts/evaluate_realtime_delay_predictions.py" in source
    assert "URBANPULSE_REALTIME_EVAL_REPORT" in source


def test_repo_commands_do_not_end_with_script_extension() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")

    assert "return f\"set -euo pipefail\\ncd {q(REPO_DIR)}\\n{command}\\n\"" in source


def test_fetch_vehicle_exports_script_supports_remote_refresh() -> None:
    source = FETCH_SCRIPT_PATH.read_text(encoding="utf-8")

    for setting in [
        "REMOTE_EXPORT_BEFORE_FETCH",
        "REMOTE_EXPORT_COMMAND",
        "REMOTE_INCREMENTAL_EXPORT",
        "REMOTE_INCREMENTAL_MANIFEST",
        "REMOTE_REPO_DIR",
        "REMOTE_EXPORT_SCRIPT",
        "CLEAN_DOWNLOAD_DIR_BEFORE_FETCH",
        "CLEAN_REMOTE_DIR_BEFORE_EXPORT",
        "CLEAN_REMOTE_DIR_AFTER_FETCH",
        "CREATE_ARCHIVE",
        "SSH_OPTS",
        "SCP_OPTS",
    ]:
        assert setting in source

    assert "run_remote_export" in source
    assert "ssh " in source
    assert "scp " in source
