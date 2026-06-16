"""Database migrations for UrbanPulse.

The project intentionally uses a small SQL-file migration runner for now:
versioned files live in `apps/data-service/migrations`, and applied versions
are tracked in the database.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import asyncpg

MIGRATION_LOCK_NAME = "urbanpulse_schema_migrations"
MIGRATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""
PLACEHOLDER_PATTERN = re.compile(r"\{\{([A-Z0-9_]+)\}\}")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    path: Path
    sql: str
    checksum: str


def _load_root_env() -> None:
    """Load variables from the repo-root `.env` into `os.environ` (best-effort)."""

    service_path = Path(__file__).resolve()
    env_candidates = [
        service_path.parent / ".env",
        *[parent / ".env" for parent in service_path.parents],
    ]
    env_path = next((path for path in env_candidates if path.exists()), None)
    if env_path is None:
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


def get_int_env(name: str, default: int) -> int:
    """Read an integer env var and fall back to a default on invalid input."""

    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default

    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; using default %d", name, raw, default)
        return default


def get_bool_env(name: str, default: bool = False) -> bool:
    """Read a boolean env var from common truthy values."""

    raw = os.getenv(name)
    if raw is None:
        return default

    return raw.strip().lower() in {"1", "true", "yes", "on"}


def get_db_url() -> str:
    """Return a Postgres connection URL from environment variables."""

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


def migration_settings() -> dict[str, str]:
    """Return sanitized settings used by idempotent Timescale setup."""

    return {
        "POSITIONS_RETENTION_DAYS": str(get_int_env("POSITIONS_RETENTION_DAYS", 35)),
        "POSITIONS_COMPRESS_AFTER_DAYS": str(get_int_env("POSITIONS_COMPRESS_AFTER_DAYS", 2)),
        "TRAJECTORY_RETENTION_DAYS": str(get_int_env("TRAJECTORY_RETENTION_DAYS", 14)),
        "TRAJECTORY_COMPRESS_AFTER_DAYS": str(get_int_env("TRAJECTORY_COMPRESS_AFTER_DAYS", 2)),
        "PREDICTIONS_RETENTION_DAYS": str(get_int_env("PREDICTIONS_RETENTION_DAYS", 7)),
        "PREDICTIONS_COMPRESS_AFTER_DAYS": str(get_int_env("PREDICTIONS_COMPRESS_AFTER_DAYS", 1)),
        "ENABLE_TRAJECTORY_HYPERTABLE_MIGRATION": (
            "true" if get_bool_env("ENABLE_TRAJECTORY_HYPERTABLE_MIGRATION") else "false"
        ),
    }


def render_sql_template(sql: str, settings: dict[str, str] | None = None) -> str:
    """Substitute known `{{NAME}}` placeholders in migration SQL."""

    values = migration_settings() if settings is None else settings

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise ValueError(f"Unknown migration placeholder: {key}")
        return values[key]

    return PLACEHOLDER_PATTERN.sub(replace, sql)


def discover_migrations(migrations_dir: Path | None = None) -> list[Migration]:
    """Load versioned SQL migration files from disk."""

    directory = migrations_dir or Path(__file__).resolve().parent / "migrations"
    if not directory.exists():
        raise FileNotFoundError(f"Migration directory does not exist: {directory}")

    migrations: list[Migration] = []
    seen_versions: set[str] = set()
    for path in sorted(directory.glob("*.sql")):
        version, _, name = path.stem.partition("_")
        if not version or not name:
            raise ValueError(f"Migration filename must be VERSION_name.sql: {path.name}")
        if version in seen_versions:
            raise ValueError(f"Duplicate migration version {version} in {directory}")
        seen_versions.add(version)

        raw_sql = path.read_text(encoding="utf-8")
        sql = render_sql_template(raw_sql)
        migrations.append(
            Migration(
                version=version,
                name=name,
                path=path,
                sql=sql,
                checksum=hashlib.sha256(raw_sql.encode("utf-8")).hexdigest(),
            )
        )

    if not migrations:
        raise ValueError(f"No migration SQL files found in {directory}")
    return migrations


async def _is_hypertable(conn: asyncpg.Connection, table_name: str) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_name = $1
            )
            """,
            table_name,
        )
    )


async def _table_has_rows(conn: asyncpg.Connection, table_name: str) -> bool:
    return bool(await conn.fetchval(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1)"))


async def _timescaledb_extension_exists(conn: asyncpg.Connection) -> bool:
    return bool(
        await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')"
        )
    )


async def _applied_migrations(conn: asyncpg.Connection) -> dict[str, str]:
    rows = await conn.fetch("SELECT version, checksum FROM schema_migrations")
    return {row["version"]: row["checksum"] for row in rows}


async def _apply_pending_migrations(
    conn: asyncpg.Connection,
    migrations: list[Migration],
) -> list[str]:
    await conn.execute(MIGRATIONS_TABLE_SQL)
    applied = await _applied_migrations(conn)
    applied_versions: list[str] = []

    for migration in migrations:
        existing_checksum = applied.get(migration.version)
        if existing_checksum is not None:
            if existing_checksum != migration.checksum:
                raise ValueError(
                    f"Applied migration {migration.version} checksum differs from "
                    f"{migration.path.name}. Create a new migration instead of editing it."
                )
            continue

        logger.info("Applying database migration %s (%s)", migration.version, migration.name)
        async with conn.transaction():
            await conn.execute(migration.sql)
            await conn.execute(
                """
                INSERT INTO schema_migrations (version, name, checksum)
                VALUES ($1, $2, $3)
                """,
                migration.version,
                migration.name,
                migration.checksum,
            )
        applied_versions.append(migration.version)

    return applied_versions


async def configure_timescale(conn: asyncpg.Connection) -> None:
    """Configure TimescaleDB hypertables, compression, and retention idempotently."""

    if not await _timescaledb_extension_exists(conn):
        logger.info("TimescaleDB extension not available; using standard Postgres tables.")
        return

    positions_retention_days = get_int_env("POSITIONS_RETENTION_DAYS", 35)
    trajectory_retention_days = get_int_env("TRAJECTORY_RETENTION_DAYS", 14)
    positions_compress_after_days = get_int_env("POSITIONS_COMPRESS_AFTER_DAYS", 2)
    trajectory_compress_after_days = get_int_env("TRAJECTORY_COMPRESS_AFTER_DAYS", 2)
    predictions_retention_days = get_int_env("PREDICTIONS_RETENTION_DAYS", 7)
    predictions_compress_after_days = get_int_env("PREDICTIONS_COMPRESS_AFTER_DAYS", 1)
    enable_traj_hypertable_migration = get_bool_env(
        "ENABLE_TRAJECTORY_HYPERTABLE_MIGRATION",
        False,
    )

    try:
        await conn.execute(
            """
            SELECT create_hypertable(
                'vehicle_positions',
                'time',
                chunk_time_interval => INTERVAL '1 day',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
            """
        )
        await conn.execute(
            """
            ALTER TABLE vehicle_positions SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'vehicle_id',
                timescaledb.compress_orderby = 'time DESC'
            );
            """
        )
        await conn.execute(
            "SELECT add_compression_policy('vehicle_positions', $1::interval, if_not_exists => TRUE);",
            timedelta(days=positions_compress_after_days),
        )
        await conn.execute(
            "SELECT add_retention_policy('vehicle_positions', $1::interval, if_not_exists => TRUE);",
            timedelta(days=positions_retention_days),
        )
        logger.info(
            "vehicle_positions hypertable is ready with compression after %s days and retention after %s days.",
            positions_compress_after_days,
            positions_retention_days,
        )
    except Exception as exc:
        logger.warning("Failed to configure vehicle_positions Timescale policies: %s", exc)

    try:
        trajectory_is_hypertable = await _is_hypertable(conn, "vehicle_trajectory_points")
        if not trajectory_is_hypertable:
            trajectory_has_rows = await _table_has_rows(conn, "vehicle_trajectory_points")
            if trajectory_has_rows and not enable_traj_hypertable_migration:
                logger.warning(
                    "vehicle_trajectory_points is still a plain table with existing data. "
                    "Set ENABLE_TRAJECTORY_HYPERTABLE_MIGRATION=true during a maintenance "
                    "window to migrate it and enable retention/compression policies."
                )
            else:
                await conn.execute(
                    """
                    SELECT create_hypertable(
                        'vehicle_trajectory_points',
                        'time',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    );
                    """
                )
                trajectory_is_hypertable = True

        if trajectory_is_hypertable:
            await conn.execute(
                """
                ALTER TABLE vehicle_trajectory_points SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'vehicle_id',
                    timescaledb.compress_orderby = 'time DESC'
                );
                """
            )
            await conn.execute(
                "SELECT add_compression_policy('vehicle_trajectory_points', $1::interval, if_not_exists => TRUE);",
                timedelta(days=trajectory_compress_after_days),
            )
            await conn.execute(
                "SELECT add_retention_policy('vehicle_trajectory_points', $1::interval, if_not_exists => TRUE);",
                timedelta(days=trajectory_retention_days),
            )
            logger.info(
                "vehicle_trajectory_points hypertable is ready with compression after %s days and retention after %s days.",
                trajectory_compress_after_days,
                trajectory_retention_days,
            )
    except Exception as exc:
        logger.warning("Failed to configure trajectory retention/compression: %s", exc)

    try:
        await conn.execute(
            """
            SELECT create_hypertable(
                'delay_increase_predictions',
                'scored_at',
                chunk_time_interval => INTERVAL '1 day',
                if_not_exists => TRUE,
                migrate_data => TRUE
            );
            """
        )
        await conn.execute(
            """
            ALTER TABLE delay_increase_predictions SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'vehicle_id',
                timescaledb.compress_orderby = 'scored_at DESC'
            );
            """
        )
        await conn.execute(
            "SELECT add_compression_policy('delay_increase_predictions', $1::interval, if_not_exists => TRUE);",
            timedelta(days=predictions_compress_after_days),
        )
        await conn.execute(
            "SELECT add_retention_policy('delay_increase_predictions', $1::interval, if_not_exists => TRUE);",
            timedelta(days=predictions_retention_days),
        )
        logger.info(
            "delay_increase_predictions hypertable is ready with compression after %s days and retention after %s days.",
            predictions_compress_after_days,
            predictions_retention_days,
        )
    except Exception as exc:
        logger.warning("Failed to configure prediction retention/compression: %s", exc)


async def run_migrations(
    pool: asyncpg.Pool,
    migrations_dir: Path | None = None,
) -> list[str]:
    """Apply pending SQL migrations and configure TimescaleDB."""

    migrations = discover_migrations(migrations_dir)

    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock(hashtext($1))", MIGRATION_LOCK_NAME)
        try:
            applied_versions = await _apply_pending_migrations(conn, migrations)
            await configure_timescale(conn)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", MIGRATION_LOCK_NAME)

    if applied_versions:
        logger.info("Applied database migrations: %s", ", ".join(applied_versions))
    else:
        logger.info("Database migrations are up to date.")
    return applied_versions


async def main() -> None:
    _load_root_env()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    pool = await asyncpg.create_pool(get_db_url(), min_size=1, max_size=1)
    try:
        await run_migrations(pool)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
