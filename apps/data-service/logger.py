"""UrbanPulse logger.

This script subscribes to Redis updates and persists them into TimescaleDB
(PostgreSQL). It is intended to be run as a standalone process.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
import redis.asyncio as redis

BATCH_SIZE = 100
FLUSH_INTERVAL = 1.0
REDIS_CHANNEL = "urban_pulse:updates"

logger = logging.getLogger(__name__)
VehicleRow = tuple[
    datetime,
    str,
    str,
    int,
    float,
    float,
    int,
    str | None,
    str | None,
    int | None,
    str | None,
    str | None,
    datetime | None,
    str | None,
    int | None,
    datetime | None,
    datetime | None,
    str | None,
    int | None,
    datetime | None,
    datetime | None,
]


def _load_root_env() -> None:
    """Load variables from the repo-root `.env` into `os.environ` (best-effort)."""

    env_path = Path(__file__).resolve().parents[2] / ".env"
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


_load_root_env()


def get_db_url() -> str:
    """Return a Postgres connection URL from environment variables.

    Prefers `DATABASE_URL` when present. Otherwise builds a URL from the
    `POSTGRES_*` variables.
    """

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


async def init_db(pool: asyncpg.Pool) -> None:
    """Create required tables and indices, and convert to a hypertable if available."""

    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vehicle_positions (
                time                      TIMESTAMPTZ       NOT NULL,
                vehicle_id                TEXT              NOT NULL,
                lat                       DOUBLE PRECISION  NOT NULL,
                lon                       DOUBLE PRECISION  NOT NULL,
                line                      TEXT,
                delay                     INTEGER,
                speed                     INTEGER,
                route_id                  TEXT,
                mode                      TEXT,
                route_type                INTEGER,
                trip_id                   TEXT,
                state_position            TEXT,
                origin_timestamp          TIMESTAMPTZ,
                last_stop_id              TEXT,
                last_stop_sequence        INTEGER,
                last_stop_arrival_time    TIMESTAMPTZ,
                last_stop_departure_time  TIMESTAMPTZ,
                next_stop_id              TEXT,
                next_stop_sequence        INTEGER,
                next_stop_arrival_time    TIMESTAMPTZ,
                next_stop_departure_time  TIMESTAMPTZ
            );
        """
        )

        # Keep existing local databases compatible when new telemetry fields are added.
        alter_statements = [
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS route_id TEXT;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS trip_id TEXT;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS mode TEXT;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS route_type INTEGER;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS state_position TEXT;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS origin_timestamp TIMESTAMPTZ;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS last_stop_id TEXT;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS last_stop_sequence INTEGER;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS last_stop_arrival_time TIMESTAMPTZ;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS last_stop_departure_time TIMESTAMPTZ;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS next_stop_id TEXT;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS next_stop_sequence INTEGER;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS next_stop_arrival_time TIMESTAMPTZ;",
            "ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS next_stop_departure_time TIMESTAMPTZ;",
        ]
        for statement in alter_statements:
            await conn.execute(statement)

        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_vehicle_id ON vehicle_positions (vehicle_id, time DESC);"
        )

        try:
            await conn.execute(
                "SELECT create_hypertable('vehicle_positions', 'time', if_not_exists => TRUE);"
            )
            logger.info("Database initialized as a hypertable.")
        except Exception:
            logger.info("TimescaleDB extension not available; using a standard table.")


async def flush_buffer(
    pool: asyncpg.Pool,
    buffer: list[VehicleRow],
) -> None:
    """Flush buffered rows to the database."""

    if not buffer:
        return

    try:
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                "vehicle_positions",
                records=buffer,
                columns=[
                    "time",
                    "vehicle_id",
                    "line",
                    "delay",
                    "lat",
                    "lon",
                    "speed",
                    "route_id",
                    "mode",
                    "route_type",
                    "trip_id",
                    "state_position",
                    "origin_timestamp",
                    "last_stop_id",
                    "last_stop_sequence",
                    "last_stop_arrival_time",
                    "last_stop_departure_time",
                    "next_stop_id",
                    "next_stop_sequence",
                    "next_stop_arrival_time",
                    "next_stop_departure_time",
                ],
                timeout=10,
            )
        logger.info("Flushed %d records to the database.", len(buffer))
    except Exception as exc:
        logger.error("Failed to flush buffer: %s", exc)


def _parse_message_data(message: dict[str, Any]) -> dict[str, Any] | None:
    """Parse and validate a Redis pubsub message."""

    if message.get("type") != "message":
        return None

    raw = message.get("data")
    if not raw:
        return None

    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    if "id" not in payload or "lat" not in payload or "lon" not in payload:
        return None

    return payload


def _to_int(value: Any, default: int = 0) -> int:
    """Convert unknown numeric input to integer, with fallback."""

    if isinstance(value, bool):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_optional_int(value: Any) -> int | None:
    """Convert unknown numeric input to integer, or None when missing/invalid."""

    if value is None or isinstance(value, bool):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_optional_text(value: Any) -> str | None:
    """Convert values to text when present."""

    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    return None


def _to_optional_timestamp(value: Any) -> datetime | None:
    """Parse ISO8601 timestamps from upstream payloads."""

    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


async def main() -> None:
    """Run the Redis-to-Postgres ingestion loop."""

    db_url = get_db_url()
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    pool: asyncpg.Pool | None = None
    client: redis.Redis | None = None

    try:
        pool = await asyncpg.create_pool(db_url)
        await init_db(pool)
        logger.info("Connected to database.")

        client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        pubsub = client.pubsub()
        await pubsub.subscribe(REDIS_CHANNEL)
        logger.info("Subscribed to Redis channel: %s", REDIS_CHANNEL)

        buffer: list[VehicleRow] = []
        last_flush_time = time.time()

        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=0.1,
            )
            if message:
                payload = _parse_message_data(message)
                if payload:
                    try:
                        buffer.append(
                            (
                                datetime.now(timezone.utc),
                                str(payload["id"]),
                                str(payload.get("line", "")),
                                _to_int(payload.get("delay", 0), default=0),
                                float(payload["lat"]),
                                float(payload["lon"]),
                                _to_int(payload.get("speed", 0), default=0),
                                _to_optional_text(payload.get("route_id")),
                                _to_optional_text(payload.get("mode")),
                                _to_optional_int(payload.get("route_type")),
                                _to_optional_text(payload.get("trip_id")),
                                _to_optional_text(payload.get("state_position")),
                                _to_optional_timestamp(payload.get("origin_timestamp")),
                                _to_optional_text(payload.get("last_stop_id")),
                                _to_optional_int(payload.get("last_stop_sequence")),
                                _to_optional_timestamp(payload.get("last_stop_arrival_time")),
                                _to_optional_timestamp(payload.get("last_stop_departure_time")),
                                _to_optional_text(payload.get("next_stop_id")),
                                _to_optional_int(payload.get("next_stop_sequence")),
                                _to_optional_timestamp(payload.get("next_stop_arrival_time")),
                                _to_optional_timestamp(payload.get("next_stop_departure_time")),
                            )
                        )
                    except (TypeError, ValueError):
                        pass

            now = time.time()
            should_flush = len(buffer) >= BATCH_SIZE or (
                buffer and now - last_flush_time > FLUSH_INTERVAL
            )
            if should_flush and pool:
                await flush_buffer(pool, buffer)
                buffer = []
                last_flush_time = now
    except asyncio.CancelledError:
        raise
    except KeyboardInterrupt:
        logger.info("Stopping logger...")
    except Exception as exc:
        logger.error("Logger failed: %s", exc)
    finally:
        if pool:
            await pool.close()
        if client:
            await client.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(main())
