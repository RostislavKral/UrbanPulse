"""UrbanPulse data service.

This service periodically fetches vehicle positions from the Golemio API and
publishes normalized updates to Redis for downstream consumers.
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from redis.asyncio import Redis

GOLEMIO_API_URL = "https://api.golemio.cz/v2/vehiclepositions?limit=3000"
REDIS_CHANNEL = "urban_pulse:updates"
PUBLISH_INTERVAL_SECONDS = 5

logger = logging.getLogger(__name__)


def _load_root_env() -> None:
    """Load variables from the repo-root `.env` into `os.environ` (best-effort).

    This keeps local runs consistent even when the service is started outside of
    Docker Compose.
    """

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

redis_client = Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)
scheduler = AsyncIOScheduler()


def _get_api_key() -> str | None:
    """Return the API key for Golemio, if configured."""

    return os.getenv("GOLEMIO_API_KEY")


async def fetch_data(client: httpx.AsyncClient) -> dict[str, Any] | None:
    """Fetch raw vehicle position data from the upstream API."""

    api_key = _get_api_key()
    if not api_key:
        logger.error("GOLEMIO_API_KEY is not set")
        return None

    headers = {"x-access-token": api_key, "Content-Type": "application/json"}
    try:
        response = await client.get(GOLEMIO_API_URL, headers=headers, timeout=10.0)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        logger.warning("Failed to fetch data: %s", exc)
        return None


async def publish_updates() -> None:
    """Fetch data and publish normalized updates to Redis."""

    logger.info("Fetching data...")
    try:
        async with httpx.AsyncClient() as client:
            data = await fetch_data(client)
    except Exception as exc:
        logger.warning("Failed to initialize HTTP client: %s", exc)
        return

    if not data:
        return

    if not isinstance(data, dict):
        logger.warning("Unexpected API response format")
        return

    features = data.get("features", [])
    if not isinstance(features, list):
        logger.warning("Unexpected features format")
        return

    for feature in features:
        if not isinstance(feature, dict):
            continue

        try:
            props = feature.get("properties") or {}
            trip = props.get("trip") or {}
            gtfs = trip.get("gtfs") or {}
            geometry = feature.get("geometry") or {}
            coords = geometry.get("coordinates") or []
            if len(coords) < 2:
                continue

            lon, lat = coords[0], coords[1]
            if lat is None or lon is None:
                continue

            vehicle_id = (
                props.get("vehicle_id")
                or trip.get("vehicle_registration_number")
                or gtfs.get("trip_id")
            )
            if not vehicle_id:
                continue

            line = props.get("route_short_name")
            if not isinstance(line, str):
                line = gtfs.get("route_short_name")
            if not isinstance(line, str):
                line = gtfs.get("route_id")
            if not isinstance(line, str):
                line = trip.get("origin_route_name")
            if not isinstance(line, str):
                line = "unknown"

            delay_value = props.get("delay")
            if not isinstance(delay_value, (int, float)):
                last_position = props.get("last_position") or {}
                delay_info = last_position.get("delay") or {}
                delay_value = delay_info.get("actual")
            if not isinstance(delay_value, (int, float)):
                delay_value = 0

            payload = {
                "id": str(vehicle_id),
                "line": line,
                "delay": delay_value,
                "lat": lat,
                "lon": lon,
            }
            await redis_client.publish(REDIS_CHANNEL, json.dumps(payload))
        except Exception as exc:
            logger.debug("Failed to process feature: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Manage application startup/shutdown resources."""

    scheduler.add_job(publish_updates, "interval", seconds=PUBLISH_INTERVAL_SECONDS)
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        await redis_client.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Healthcheck endpoint."""

    return {"status": "ok"}
