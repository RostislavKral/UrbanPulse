"""UrbanPulse data service.

This service periodically fetches vehicle positions from the Golemio API and
publishes normalized updates to Redis for downstream consumers.
"""

import json
import logging
import os
from csv import DictReader
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
# GTFS route_type -> human-readable transport mode.
ROUTE_TYPE_TO_MODE = {
    0: "tram",
    1: "metro",
    2: "rail",
    3: "bus",
    4: "ferry",
    11: "trolleybus",
}

ROUTE_TYPE_BY_ID: dict[str, int] = {}

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
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
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


def load_route_type_by_id() -> dict[str, int]:
    """Build a route_id -> route_type mapping from GTFS routes.txt."""

    candidates = [
        Path(__file__).resolve().parents[2] / "db/gtfs/pid_static/routes.txt",
        Path("/app/db/gtfs/pid_static/routes.txt"),
    ]

    for routes_path in candidates:
        if not routes_path.exists():
            continue
        try:
            route_type_by_id: dict[str, int] = {}
            with routes_path.open(encoding="utf-8", newline="") as csv_file:
                reader = DictReader(csv_file)
                for row in reader:
                    route_id = str(row.get("route_id") or "").strip()
                    route_type_raw = row.get("route_type")
                    if not route_id:
                        continue
                    try:
                        route_type = int(str(route_type_raw))
                    except (TypeError, ValueError):
                        continue
                    route_type_by_id[route_id] = route_type

            logger.info(
                "Loaded %d GTFS route mappings from %s",
                len(route_type_by_id),
                routes_path,
            )
            return route_type_by_id
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", routes_path, exc)

    logger.warning("GTFS routes.txt not found; route type mapping is empty.")
    return {}


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

            last_position = props.get("last_position") or {}
            if not isinstance(last_position, dict):
                last_position = {}

            delay_value = props.get("delay")
            if not isinstance(delay_value, (int, float)):
                delay_info = last_position.get("delay") or {}
                delay_value = delay_info.get("actual")
            if not isinstance(delay_value, (int, float)):
                delay_value = 0

            speed_value = last_position.get("speed")
            if not isinstance(speed_value, (int, float)):
                speed_value = None

            state_position = last_position.get("state_position")
            if not isinstance(state_position, str):
                state_position = None

            last_stop = last_position.get("last_stop") or {}
            if not isinstance(last_stop, dict):
                last_stop = {}

            next_stop = last_position.get("next_stop") or {}
            if not isinstance(next_stop, dict):
                next_stop = {}

            route_id_raw = gtfs.get("route_id")
            route_id = (
                route_id_raw.strip()
                if isinstance(route_id_raw, str) and route_id_raw.strip()
                else None
            )

            route_type: int | None = None
            route_type_raw = gtfs.get("route_type")
            if isinstance(route_type_raw, (int, float)) and not isinstance(route_type_raw, bool):
                route_type = int(route_type_raw)
            elif isinstance(route_type_raw, str):
                try:
                    route_type = int(route_type_raw)
                except ValueError:
                    route_type = None

            if route_type is None and route_id:
                route_type = ROUTE_TYPE_BY_ID.get(route_id)

            mode = ROUTE_TYPE_TO_MODE.get(route_type, "unknown")

            payload = {
                "id": str(vehicle_id),
                "line": line,
                "delay": delay_value,
                "lat": lat,
                "lon": lon,
                "speed": speed_value,
                "route_id": route_id,
                "trip_id": gtfs.get("trip_id"),
                "route_type": route_type,
                "mode": mode,
                "state_position": state_position,
                "origin_timestamp": last_position.get("origin_timestamp"),
                "last_stop_id": last_stop.get("id"),
                "last_stop_sequence": last_stop.get("sequence"),
                "last_stop_arrival_time": last_stop.get("arrival_time"),
                "last_stop_departure_time": last_stop.get("departure_time"),
                "next_stop_id": next_stop.get("id"),
                "next_stop_sequence": next_stop.get("sequence"),
                "next_stop_arrival_time": next_stop.get("arrival_time"),
                "next_stop_departure_time": next_stop.get("departure_time"),
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


ROUTE_TYPE_BY_ID = load_route_type_by_id()
app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Healthcheck endpoint."""

    return {"status": "ok"}
