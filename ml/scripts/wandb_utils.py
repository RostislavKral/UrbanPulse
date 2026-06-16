from __future__ import annotations

import os
import shlex
from pathlib import Path
from typing import Any

FALSE_VALUES = {"0", "false", "no", "off", "disabled"}
TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
OPTIONAL_WANDB_ENV_KEYS = [
    "WANDB_MODE",
    "WANDB_ENTITY",
    "WANDB_RUN_GROUP",
    "WANDB_RUN_NAME",
]


def load_root_env(repo_root: Path) -> None:
    """Load missing values from the repo-root `.env` file."""

    env_path = repo_root / ".env"
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

        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue

        try:
            value = shlex.split(raw_value, comments=False, posix=True)[0]
        except (IndexError, ValueError):
            value = raw_value.strip().strip("'\"")
        os.environ[key] = value


def wandb_enabled() -> bool:
    enabled = os.getenv("WANDB_ENABLED", "").strip().lower()
    mode = os.getenv("WANDB_MODE", "").strip().lower()

    if enabled in FALSE_VALUES or mode in FALSE_VALUES:
        return False
    if enabled in TRUE_VALUES:
        return True
    return bool(os.getenv("WANDB_API_KEY") or mode in {"online", "offline", "dryrun"})


def sanitize_wandb_environment() -> None:
    for key in OPTIONAL_WANDB_ENV_KEYS:
        if os.getenv(key) == "":
            os.environ.pop(key)


def init_wandb_run(
    *,
    repo_root: Path,
    job_type: str,
    config: dict[str, Any],
    tags: list[str] | None = None,
):
    load_root_env(repo_root)
    if not wandb_enabled():
        return None

    sanitize_wandb_environment()
    try:
        import wandb
    except ImportError:
        print("W&B logging requested, but the `wandb` package is not installed.")
        return None

    project = os.getenv("WANDB_PROJECT", "urbanpulse")
    entity = os.getenv("WANDB_ENTITY") or None
    group = os.getenv("WANDB_RUN_GROUP") or None
    name = os.getenv("WANDB_RUN_NAME") or None

    try:
        run = wandb.init(
            project=project,
            entity=entity,
            job_type=job_type,
            group=group,
            name=name,
            config=config,
            tags=tags,
        )
        print(f"W&B run initialized: {run.url}")
        return run
    except Exception as exc:
        print(f"W&B initialization skipped: {exc}")
        return None


def log_metrics(run, metrics: dict[str, Any], *, step: int | None = None) -> None:
    if run is None:
        return

    clean_metrics = {
        key: value
        for key, value in metrics.items()
        if value is not None
    }
    if clean_metrics:
        if step is None:
            run.log(clean_metrics)
        else:
            run.log(clean_metrics, step=step)


def log_table(run, name: str, rows: list[dict[str, Any]]) -> None:
    if run is None or not rows:
        return

    import wandb

    columns = list(rows[0].keys())
    data = [[row.get(column) for column in columns] for row in rows]
    run.log({name: wandb.Table(columns=columns, data=data)})


def log_file_artifact(
    run,
    path: Path,
    *,
    name: str,
    artifact_type: str,
    aliases: list[str] | None = None,
) -> None:
    if run is None or not path.exists():
        return

    import wandb

    artifact = wandb.Artifact(name=name, type=artifact_type)
    artifact.add_file(str(path))
    run.log_artifact(artifact, aliases=aliases)
