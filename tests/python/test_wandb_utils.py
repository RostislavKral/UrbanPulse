import os

from ml.scripts.wandb_utils import (
    load_root_env,
    log_metrics,
    sanitize_wandb_environment,
    wandb_enabled,
)


class DummyRun:
    def __init__(self) -> None:
        self.logged = []

    def log(self, metrics, **kwargs) -> None:
        self.logged.append((metrics, kwargs))


def test_load_root_env_reads_missing_values_without_overwriting(
    monkeypatch,
    tmp_path,
) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "WANDB_API_KEY=from-file\n"
        "WANDB_PROJECT='urban pulse test'\n"
        "EXISTING_VALUE=from-file\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXISTING_VALUE", "already-set")
    monkeypatch.delenv("WANDB_API_KEY", raising=False)
    monkeypatch.delenv("WANDB_PROJECT", raising=False)

    load_root_env(tmp_path)

    assert os.environ["WANDB_API_KEY"] == "from-file"
    assert os.environ["WANDB_PROJECT"] == "urban pulse test"
    assert os.environ["EXISTING_VALUE"] == "already-set"


def test_wandb_enabled_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("WANDB_API_KEY", "set")
    monkeypatch.setenv("WANDB_MODE", "disabled")
    monkeypatch.delenv("WANDB_ENABLED", raising=False)

    assert wandb_enabled() is False


def test_wandb_enabled_with_api_key(monkeypatch) -> None:
    monkeypatch.setenv("WANDB_API_KEY", "set")
    monkeypatch.delenv("WANDB_MODE", raising=False)
    monkeypatch.delenv("WANDB_ENABLED", raising=False)

    assert wandb_enabled() is True


def test_sanitize_wandb_environment_removes_empty_optional_values(monkeypatch) -> None:
    monkeypatch.setenv("WANDB_MODE", "")
    monkeypatch.setenv("WANDB_ENTITY", "")
    monkeypatch.setenv("WANDB_PROJECT", "urbanpulse")

    sanitize_wandb_environment()

    assert "WANDB_MODE" not in os.environ
    assert "WANDB_ENTITY" not in os.environ
    assert os.environ["WANDB_PROJECT"] == "urbanpulse"


def test_log_metrics_filters_empty_values_and_preserves_step() -> None:
    run = DummyRun()

    log_metrics(run, {"score": 0.5, "missing": None}, step=25)

    assert run.logged == [({"score": 0.5}, {"step": 25})]
