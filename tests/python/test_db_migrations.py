from pathlib import Path

import pytest
from db_migrations import discover_migrations, render_sql_template


def test_render_sql_template_replaces_known_placeholders() -> None:
    rendered = render_sql_template(
        "SELECT INTERVAL '{{POSITIONS_RETENTION_DAYS}} days', "
        "{{ENABLE_TRAJECTORY_HYPERTABLE_MIGRATION}};",
        {
            "POSITIONS_RETENTION_DAYS": "40",
            "ENABLE_TRAJECTORY_HYPERTABLE_MIGRATION": "true",
        },
    )

    assert rendered == "SELECT INTERVAL '40 days', true;"


def test_render_sql_template_rejects_unknown_placeholders() -> None:
    with pytest.raises(ValueError, match="UNKNOWN_SETTING"):
        render_sql_template("SELECT {{UNKNOWN_SETTING}};", {})


def test_discover_migrations_orders_files_and_extracts_metadata(tmp_path: Path) -> None:
    (tmp_path / "002_second.sql").write_text("SELECT 2;", encoding="utf-8")
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf-8")

    migrations = discover_migrations(tmp_path)

    assert [migration.version for migration in migrations] == ["001", "002"]
    assert [migration.name for migration in migrations] == ["first", "second"]
    assert all(len(migration.checksum) == 64 for migration in migrations)


def test_discover_migrations_rejects_duplicate_versions(tmp_path: Path) -> None:
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "001_other.sql").write_text("SELECT 2;", encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate migration version 001"):
        discover_migrations(tmp_path)
