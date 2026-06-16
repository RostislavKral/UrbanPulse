import db_migrations
import logger
import main
import pytest


def clear_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in [
        "DATABASE_URL",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
    ]:
        monkeypatch.delenv(key, raising=False)


def test_get_db_url_prefers_explicit_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_db_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql://explicit/db")

    assert logger.get_db_url() == "postgresql://explicit/db"
    assert main._get_db_url() == "postgresql://explicit/db"


def test_get_db_url_builds_from_postgres_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_db_env(monkeypatch)
    monkeypatch.setenv("POSTGRES_USER", "admin")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_HOST", "timescaledb")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "prague_transport")

    expected = "postgresql://admin:secret@timescaledb:5432/prague_transport"
    assert logger.get_db_url() == expected
    assert main._get_db_url() == expected


def test_get_db_url_reports_missing_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_db_env(monkeypatch)

    with pytest.raises(ValueError, match="POSTGRES_USER"):
        logger.get_db_url()


def test_migration_env_parsers_fall_back_on_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSITIONS_RETENTION_DAYS", "not-an-int")
    monkeypatch.setenv("ENABLE_TRAJECTORY_HYPERTABLE_MIGRATION", "yes")

    assert db_migrations.get_int_env("POSITIONS_RETENTION_DAYS", 35) == 35
    assert db_migrations.get_bool_env("ENABLE_TRAJECTORY_HYPERTABLE_MIGRATION") is True
