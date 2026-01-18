import json
import os
from typing import Any

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from redis.asyncio import Redis

GOLEMIO_API_URL = "https://api.golemio.cz/v2/vehiclepositions?limit=3000"

app = FastAPI()
redis_client = Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)
scheduler = AsyncIOScheduler()

def _get_api_key() -> str | None:
    return os.getenv("GOLEMIO_API_KEY")


async def fetch_data(client: httpx.AsyncClient) -> dict[str, Any] | None:
    api_key = _get_api_key()
    if not api_key:
        print("GOLEMIO_API_KEY is not set", flush=True)
        return None

    headers = {"x-access-token": api_key, "Content-Type": "application/json"}
    try:
        response = await client.get(GOLEMIO_API_URL, headers=headers, timeout=10.0)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"Failed to fetch data: {exc}", flush=True)
        return None


async def publish_updates() -> None:
    print("Fetching data...", flush=True)
    try:
        async with httpx.AsyncClient() as client:
            data = await fetch_data(client)
    except Exception as exc:
        print(f"Failed to initialize HTTP client: {exc}", flush=True)
        return

    if not data:
        return

    if not isinstance(data, dict):
        print("Unexpected API response format", flush=True)
        return

    features = data.get("features", [])
    if not isinstance(features, list):
        print("Unexpected features format", flush=True)
        return
    for feature in features:
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
            await redis_client.publish("urban_pulse:updates", json.dumps(payload))
        except Exception as exc:
            print(f"Failed to process feature: {exc}", flush=True)


@app.on_event("startup")
async def startup() -> None:
    scheduler.add_job(publish_updates, "interval", seconds=5)
    scheduler.start()


@app.on_event("shutdown")
async def shutdown() -> None:
    scheduler.shutdown(wait=False)
    await redis_client.close()
