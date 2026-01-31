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
                time        TIMESTAMPTZ       NOT NULL,
                vehicle_id  TEXT              NOT NULL,
                lat         DOUBLE PRECISION  NOT NULL,
                lon         DOUBLE PRECISION  NOT NULL,
                line        TEXT,
                delay       INTEGER,
                speed       INTEGER
            );
        """
        )

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
    buffer: list[tuple[datetime, str, str, int, float, float, int]],
) -> None:
    """Flush buffered rows to the database."""

    if not buffer:
        return

    try:
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                "vehicle_positions",
                records=buffer,
                columns=["time", "vehicle_id", "line", "delay", "lat", "lon", "speed"],
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

        buffer: list[tuple[datetime, str, str, int, float, float, int]] = []
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
                                int(payload.get("delay", 0)),
                                float(payload["lat"]),
                                float(payload["lon"]),
                                0,
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
