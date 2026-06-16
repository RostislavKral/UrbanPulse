"""UrbanPulse data service.

This service periodically fetches vehicle positions from the Golemio API and
publishes normalized updates to Redis for downstream consumers.
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from csv import DictReader
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

import asyncpg
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db_migrations import run_migrations
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

GOLEMIO_API_URL = "https://api.golemio.cz/v2/vehiclepositions?limit=3000"
REDIS_CHANNEL = "urban_pulse:updates"
PUBLISH_INTERVAL_SECONDS = 5
DEFAULT_DELAY_ALERTS_PATH = "/app/ml/models/delay_increase_alerts.json"
LOCAL_DELAY_ALERTS_PATH = (
    Path(__file__).resolve().parents[2]
    / "ml"
    / "models"
    / "delay_increase_alerts.json"
)
DEFAULT_DELAY_MODEL_PATH = "/app/ml/models/delay_increase_hgb_5min.joblib"
LOCAL_DELAY_MODEL_PATH = (
    Path(__file__).resolve().parents[2]
    / "ml"
    / "models"
    / "delay_increase_hgb_5min.joblib"
)
DEFAULT_PIPELINE_QUALITY_REPORT_PATH = "/app/ml/reports/data_quality_latest.json"
LOCAL_PIPELINE_QUALITY_REPORT_PATH = (
    Path(__file__).resolve().parents[2]
    / "ml"
    / "reports"
    / "data_quality_latest.json"
)


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; using default %d", name, raw, default)
        return default


def _get_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r; using default %s", name, raw, default)
        return default


def _get_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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


def _load_root_env() -> None:
    """Load variables from the repo-root `.env` into `os.environ` (best-effort).

    This keeps local runs consistent even when the service is started outside of
    Docker Compose.
    """

    service_path = Path(__file__).resolve()
    env_candidates = [
        service_path.parent / ".env",
        *[
            parent / ".env"
            for parent in service_path.parents
        ],
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


_load_root_env()

LIVE_FRESHNESS_SECONDS = _get_int_env("LIVE_FRESHNESS_SECONDS", 120)
REPLAY_FRESHNESS_SECONDS = _get_int_env("REPLAY_FRESHNESS_SECONDS", 600)
ALERT_ARTIFACT_FRESHNESS_SECONDS = _get_int_env(
    "ALERT_ARTIFACT_FRESHNESS_SECONDS",
    86400,
)
ALERT_ROW_FRESHNESS_SECONDS = _get_int_env("ALERT_ROW_FRESHNESS_SECONDS", 21600)
PIPELINE_REPORT_FRESHNESS_SECONDS = _get_int_env(
    "PIPELINE_REPORT_FRESHNESS_SECONDS",
    86400,
)
QUALITY_WINDOW_SECONDS = _get_int_env("QUALITY_WINDOW_SECONDS", 900)
REALTIME_INFERENCE_ENABLED = (
    os.getenv("REALTIME_INFERENCE_ENABLED", "true").strip().lower() != "false"
)
REALTIME_INFERENCE_INTERVAL_SECONDS = _get_int_env(
    "REALTIME_INFERENCE_INTERVAL_SECONDS",
    60,
)
REALTIME_INFERENCE_CONTEXT_MINUTES = _get_int_env(
    "REALTIME_INFERENCE_CONTEXT_MINUTES",
    20,
)
REALTIME_INFERENCE_OUTPUT_FRESHNESS_SECONDS = _get_int_env(
    "REALTIME_INFERENCE_OUTPUT_FRESHNESS_SECONDS",
    180,
)
REALTIME_INFERENCE_SAMPLING_SECONDS = _get_int_env(
    "REALTIME_INFERENCE_SAMPLING_SECONDS",
    30,
)
REALTIME_INFERENCE_MAX_ROWS = _get_int_env("REALTIME_INFERENCE_MAX_ROWS", 5000)
REALTIME_INFERENCE_STALE_SECONDS = _get_int_env(
    "REALTIME_INFERENCE_STALE_SECONDS",
    max(180, REALTIME_INFERENCE_INTERVAL_SECONDS * 3),
)
REALTIME_ALERT_MAX_PER_RUN = _get_int_env("REALTIME_ALERT_MAX_PER_RUN", 25)
REALTIME_ALERT_MIN_RISK = _get_float_env("REALTIME_ALERT_MIN_RISK", 0.8)
REALTIME_PREDICTION_PERSISTENCE_ENABLED = _get_bool_env(
    "REALTIME_PREDICTION_PERSISTENCE_ENABLED",
    True,
)
REALTIME_PREDICTION_MAX_PERSISTED_PER_RUN = _get_int_env(
    "REALTIME_PREDICTION_MAX_PERSISTED_PER_RUN",
    500,
)
REALTIME_PREDICTION_RETENTION_HOURS = _get_int_env(
    "REALTIME_PREDICTION_RETENTION_HOURS",
    72,
)

RealtimeAlertSnapshot = dict[str, Any]
REALTIME_ALERT_SNAPSHOT: RealtimeAlertSnapshot | None = None
REALTIME_ALERT_ERROR: str | None = None
DELAY_MODEL_CACHE: dict[str, Any] = {}

redis_client = Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)
scheduler = AsyncIOScheduler()


def _get_api_key() -> str | None:
    """Return the API key for Golemio, if configured."""

    return os.getenv("GOLEMIO_API_KEY")


def _get_db_url() -> str:
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

    service_path = Path(__file__).resolve()
    candidates = [
        *[
            parent / "db/gtfs/pid_static/routes.txt"
            for parent in service_path.parents
        ],
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
                "ingest_ts": datetime.now(UTC).isoformat(),
            }

            payload["observation_ts"] = payload.get("origin_timestamp") or payload["ingest_ts"]
            await redis_client.publish(REDIS_CHANNEL, json.dumps(payload))
        except Exception as exc:
            logger.debug("Failed to process feature: %s", exc)


async def score_realtime_delay_alerts(app: FastAPI) -> None:
    """Score recent vehicle context with the saved delay-increase model."""

    db_pool = getattr(app.state, "db_pool", None)
    if db_pool is None:
        _set_realtime_alert_error("Database pool is not initialized.")
        return

    try:
        artifact = _load_delay_model_artifact()
        rows = await _load_realtime_feature_rows(db_pool)
        scored_rows = _score_realtime_feature_rows(rows, artifact)
        alert_policy = _apply_realtime_alert_policy(scored_rows, artifact["threshold"])
        persisted_count = await _persist_realtime_predictions(db_pool, scored_rows, artifact)
        _set_realtime_alert_snapshot(
            scored_rows,
            artifact,
            {**alert_policy, "persisted_prediction_count": persisted_count},
        )
        logger.info(
            "Realtime delay inference scored %d rows, %d alerts, persisted %d predictions.",
            len(scored_rows),
            sum(1 for row in scored_rows if row.get("delay_increase_alert")),
            persisted_count,
        )
    except Exception as exc:
        _set_realtime_alert_error(str(exc))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup/shutdown resources."""

    app.state.db_pool = await asyncpg.create_pool(_get_db_url(), min_size=1, max_size=10)
    await run_migrations(app.state.db_pool)
    scheduler.add_job(publish_updates, "interval", seconds=PUBLISH_INTERVAL_SECONDS)
    if REALTIME_INFERENCE_ENABLED:
        scheduler.add_job(
            score_realtime_delay_alerts,
            "interval",
            args=[app],
            seconds=REALTIME_INFERENCE_INTERVAL_SECONDS,
            next_run_time=datetime.now(UTC),
            coalesce=True,
            max_instances=1,
        )
    else:
        logger.info("Realtime delay inference is disabled.")
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        await redis_client.close()
        db_pool = getattr(app.state, "db_pool", None)
        if db_pool is not None:
            await db_pool.close()


ROUTE_TYPE_BY_ID = load_route_type_by_id()
app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, str]:
    """Healthcheck endpoint."""

    return {"status": "ok"}


def _get_delay_alerts_path() -> Path:
    configured_path = os.getenv("DELAY_ALERTS_PATH")
    if configured_path:
        return Path(configured_path)

    container_path = Path(DEFAULT_DELAY_ALERTS_PATH)
    if container_path.exists():
        return container_path

    return LOCAL_DELAY_ALERTS_PATH


def _get_delay_model_path() -> Path:
    configured_path = os.getenv("REALTIME_DELAY_MODEL_PATH") or os.getenv("DELAY_MODEL_PATH")
    if configured_path:
        return Path(configured_path)

    container_path = Path(DEFAULT_DELAY_MODEL_PATH)
    if container_path.exists():
        return container_path

    return LOCAL_DELAY_MODEL_PATH


def _get_pipeline_quality_report_path() -> Path:
    configured_path = os.getenv("PIPELINE_QUALITY_REPORT_PATH")
    if configured_path:
        return Path(configured_path)

    container_path = Path(DEFAULT_PIPELINE_QUALITY_REPORT_PATH)
    if container_path.exists():
        return container_path

    return LOCAL_PIPELINE_QUALITY_REPORT_PATH


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _age_seconds(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    return max(0.0, (now - value).total_seconds())


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _source_status(
    latest_time: datetime | None,
    age_seconds: float | None,
    freshness_seconds: int,
    rows_near_latest: int = 0,
) -> tuple[str, str]:
    if latest_time is None:
        return "missing", "No rows have been written yet."
    if age_seconds is None:
        return "stale", "The latest timestamp could not be evaluated."
    if rows_near_latest <= 0:
        return "missing", "No rows were found near the latest timestamp."
    if age_seconds > freshness_seconds:
        return (
            "stale",
            f"Latest row is {round(age_seconds)}s old; threshold is {freshness_seconds}s.",
        )
    return "fresh", "Latest rows are within the freshness threshold."


def _overall_quality_status(source_statuses: list[str]) -> str:
    for status in ["error", "missing", "stale"]:
        if status in source_statuses:
            return status
    return "fresh"


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


REALTIME_FEATURE_SQL = """
WITH bounds AS (
    SELECT max(time) AS latest_time
    FROM vehicle_positions
),
raw AS (
    SELECT v.*
    FROM vehicle_positions v
    CROSS JOIN bounds b
    WHERE
        b.latest_time IS NOT NULL
        AND v.time >= b.latest_time - $1::interval
        AND v.delay IS NOT NULL
        AND v.lat BETWEEN 49.9 AND 50.2
        AND v.lon BETWEEN 14.2 AND 14.75
),
bucketed AS (
    SELECT
        *,
        date_bin($2::interval, time, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS time_bucket
    FROM raw
),
sampled AS (
    SELECT DISTINCT ON (vehicle_id, time_bucket)
        vehicle_id,
        time_bucket,
        time,
        delay,
        speed,
        lat,
        lon,
        mode,
        line,
        route_id,
        trip_id,
        state_position,
        route_type,
        origin_timestamp,
        last_stop_id,
        last_stop_sequence,
        last_stop_arrival_time,
        last_stop_departure_time,
        next_stop_id,
        next_stop_sequence,
        next_stop_arrival_time,
        next_stop_departure_time
    FROM bucketed
    ORDER BY vehicle_id, time_bucket, time DESC
),
features AS (
    SELECT
        *,
        time::date::text AS service_date,
        date_part('hour', time)::INTEGER AS hour_of_day,
        date_part('dow', time)::INTEGER AS day_of_week,
        lag(delay, 1) OVER vehicle_window AS delay_lag_1,
        lag(delay, 2) OVER vehicle_window AS delay_lag_2,
        lag(delay, 3) OVER vehicle_window AS delay_lag_3,
        lag(speed, 1) OVER vehicle_window AS speed_lag_1,
        lag(speed, 2) OVER vehicle_window AS speed_lag_2,
        lag(speed, 3) OVER vehicle_window AS speed_lag_3,
        lag(lat, 1) OVER vehicle_window AS lat_lag_1,
        lag(lon, 1) OVER vehicle_window AS lon_lag_1
    FROM sampled
    WINDOW vehicle_window AS (PARTITION BY vehicle_id ORDER BY time)
),
latest AS (
    SELECT DISTINCT ON (vehicle_id)
        vehicle_id,
        time,
        service_date,
        NULL::TIMESTAMPTZ AS target_time,
        NULL::TIMESTAMPTZ AS future_time,
        NULL::INTEGER AS target_lookup_lag_seconds,
        delay,
        NULL::INTEGER AS target_delay,
        NULL::INTEGER AS target_delay_delta,
        speed,
        lat,
        lon,
        lat_lag_1,
        lon_lag_1,
        delay_lag_1,
        delay_lag_2,
        delay_lag_3,
        speed_lag_1,
        speed_lag_2,
        speed_lag_3,
        delay - delay_lag_1 AS delay_delta_1,
        speed - speed_lag_1 AS speed_delta_1,
        (delay + delay_lag_1 + delay_lag_2) / 3.0 AS delay_mean_3,
        (speed + speed_lag_1 + speed_lag_2) / 3.0 AS speed_mean_3,
        mode,
        line,
        route_id,
        trip_id,
        state_position,
        route_type,
        last_stop_id,
        last_stop_sequence,
        next_stop_id,
        next_stop_sequence,
        next_stop_sequence - last_stop_sequence AS stop_sequence_gap,
        EXTRACT(EPOCH FROM time - origin_timestamp)::INTEGER AS seconds_since_origin,
        EXTRACT(EPOCH FROM time - last_stop_arrival_time)::INTEGER
            AS seconds_since_last_stop_arrival,
        EXTRACT(EPOCH FROM time - last_stop_departure_time)::INTEGER
            AS seconds_since_last_stop_departure,
        EXTRACT(EPOCH FROM next_stop_arrival_time - time)::INTEGER
            AS seconds_until_next_stop_arrival,
        EXTRACT(EPOCH FROM next_stop_departure_time - time)::INTEGER
            AS seconds_until_next_stop_departure,
        EXTRACT(EPOCH FROM next_stop_departure_time - next_stop_arrival_time)::INTEGER
            AS scheduled_next_stop_dwell_seconds,
        hour_of_day,
        day_of_week
    FROM features
    CROSS JOIN bounds b
    WHERE
        delay_lag_1 IS NOT NULL
        AND time >= b.latest_time - $4::interval
    ORDER BY vehicle_id, time DESC
)
SELECT *
FROM latest
ORDER BY time DESC
LIMIT $3
"""


def _load_delay_model_artifact() -> dict[str, Any]:
    model_path = _get_delay_model_path()
    if not model_path.exists():
        raise FileNotFoundError(f"Realtime delay model does not exist: {model_path}")

    mtime = model_path.stat().st_mtime
    cached_path = DELAY_MODEL_CACHE.get("path")
    cached_mtime = DELAY_MODEL_CACHE.get("mtime")
    if cached_path == str(model_path) and cached_mtime == mtime:
        return DELAY_MODEL_CACHE

    import joblib

    artifact = joblib.load(model_path)
    if not isinstance(artifact, dict) or "model" not in artifact:
        raise ValueError(f"{model_path} is not a supported delay-increase model artifact.")

    metadata = artifact.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    feature_columns = metadata.get("feature_columns")
    if not isinstance(feature_columns, list) or not feature_columns:
        raise ValueError("Delay model metadata does not include feature_columns.")

    threshold = metadata.get("precision_threshold")
    if not isinstance(threshold, int | float):
        threshold = 0.5

    DELAY_MODEL_CACHE.clear()
    DELAY_MODEL_CACHE.update(
        {
            "path": str(model_path),
            "mtime": mtime,
            "model": artifact["model"],
            "metadata": metadata,
            "feature_columns": [str(column) for column in feature_columns],
            "numeric_columns": [
                str(column)
                for column in metadata.get("numeric_columns", [])
                if isinstance(column, str)
            ],
            "categorical_columns": [
                str(column)
                for column in metadata.get("categorical_columns", [])
                if isinstance(column, str)
            ],
            "threshold": float(threshold),
        }
    )
    logger.info("Loaded realtime delay model from %s", model_path)
    return DELAY_MODEL_CACHE


async def _load_realtime_feature_rows(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    context = timedelta(minutes=REALTIME_INFERENCE_CONTEXT_MINUTES)
    sampling = timedelta(seconds=REALTIME_INFERENCE_SAMPLING_SECONDS)
    output_freshness = timedelta(seconds=REALTIME_INFERENCE_OUTPUT_FRESHNESS_SECONDS)
    async with pool.acquire() as conn:
        records = await conn.fetch(
            REALTIME_FEATURE_SQL,
            context,
            sampling,
            REALTIME_INFERENCE_MAX_ROWS,
            output_freshness,
        )
    return [dict(record) for record in records]


def _score_realtime_feature_rows(
    rows: list[dict[str, Any]],
    artifact: dict[str, Any],
) -> list[dict[str, Any]]:
    if not rows:
        return []

    import pandas as pd

    feature_columns = artifact["feature_columns"]
    numeric_columns = set(artifact.get("numeric_columns") or [])
    categorical_columns = set(artifact.get("categorical_columns") or [])
    model = artifact["model"]
    threshold = artifact["threshold"]

    frame = pd.DataFrame(rows)
    for column in feature_columns:
        if column not in frame.columns:
            frame[column] = None
    for column in categorical_columns:
        if column in frame.columns:
            frame[column] = frame[column].fillna("unknown")
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    scores = model.predict_proba(frame[feature_columns])[:, 1]
    scored_at = datetime.now(UTC).isoformat()
    scored_rows = []
    for row, score in zip(rows, scores, strict=False):
        risk = float(score)
        scored_rows.append(
            {
                **{key: _json_safe_value(value) for key, value in row.items()},
                "delay_increase_risk": risk,
                "raw_delay_increase_alert": risk >= threshold,
                "delay_increase_alert": risk >= threshold,
                "alert_policy_reason": "raw_threshold",
                "alert_rank": None,
                "actual_increase_60s": None,
                "scored_at": scored_at,
                "score_source": "realtime",
            }
        )

    scored_rows.sort(key=lambda item: item["delay_increase_risk"], reverse=True)
    return scored_rows


def _apply_realtime_alert_policy(
    records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Apply runtime alert guardrails while keeping every row's risk score."""

    min_risk = max(0.0, REALTIME_ALERT_MIN_RISK)
    max_alerts = REALTIME_ALERT_MAX_PER_RUN
    raw_alert_count = 0
    eligible_records: list[dict[str, Any]] = []

    for record in records:
        risk = record.get("delay_increase_risk")
        try:
            risk_value = float(risk)
        except (TypeError, ValueError):
            risk_value = 0.0

        raw_alert = risk_value >= threshold
        record["raw_delay_increase_alert"] = raw_alert
        record["delay_increase_alert"] = False
        record["alert_rank"] = None

        if not raw_alert:
            record["alert_policy_reason"] = "below_threshold"
            continue

        raw_alert_count += 1
        if risk_value < min_risk:
            record["alert_policy_reason"] = "below_min_risk"
            continue

        record["alert_policy_reason"] = "eligible"
        eligible_records.append(record)

    eligible_records.sort(
        key=lambda item: item.get("delay_increase_risk", 0.0),
        reverse=True,
    )
    selected_records = (
        eligible_records
        if max_alerts <= 0
        else eligible_records[:max_alerts]
    )
    selected_ids = {id(record) for record in selected_records}

    for rank, record in enumerate(selected_records, start=1):
        record["delay_increase_alert"] = True
        record["alert_policy_reason"] = "selected"
        record["alert_rank"] = rank

    for record in eligible_records:
        if id(record) not in selected_ids:
            record["alert_policy_reason"] = "suppressed_by_cap"

    return {
        "alert_max_per_run": max_alerts,
        "alert_min_risk": min_risk,
        "raw_alert_count": raw_alert_count,
        "eligible_alert_count": len(eligible_records),
        "suppressed_alert_count": max(0, len(eligible_records) - len(selected_records)),
    }


PREDICTION_INSERT_COLUMNS = [
    "scored_at",
    "vehicle_id",
    "position_time",
    "delay_increase_risk",
    "raw_delay_increase_alert",
    "delay_increase_alert",
    "alert_policy_reason",
    "alert_rank",
    "threshold",
    "alert_min_risk",
    "model_path",
    "model_mtime",
    "score_source",
    "context_minutes",
    "sampling_seconds",
    "output_freshness_seconds",
    "line",
    "route_id",
    "trip_id",
    "mode",
    "state_position",
    "delay",
    "actual_increase_60s",
    "delay_delta_1",
    "delay_mean_3",
    "speed",
    "speed_delta_1",
    "last_stop_id",
    "next_stop_id",
    "last_stop_sequence",
    "next_stop_sequence",
    "lat",
    "lon",
]


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _prediction_insert_tuple(
    record: dict[str, Any],
    artifact: dict[str, Any],
) -> tuple[Any, ...] | None:
    scored_at = _parse_datetime(record.get("scored_at"))
    position_time = _parse_datetime(record.get("time"))
    vehicle_id = record.get("vehicle_id")
    if scored_at is None or position_time is None or not vehicle_id:
        return None

    return (
        scored_at,
        str(vehicle_id),
        position_time,
        float(record.get("delay_increase_risk") or 0.0),
        bool(record.get("raw_delay_increase_alert")),
        bool(record.get("delay_increase_alert")),
        str(record.get("alert_policy_reason") or ""),
        _optional_int(record.get("alert_rank")),
        float(artifact["threshold"]),
        max(0.0, REALTIME_ALERT_MIN_RISK),
        str(artifact.get("path") or ""),
        _optional_float(artifact.get("mtime")),
        str(record.get("score_source") or "realtime"),
        REALTIME_INFERENCE_CONTEXT_MINUTES,
        REALTIME_INFERENCE_SAMPLING_SECONDS,
        REALTIME_INFERENCE_OUTPUT_FRESHNESS_SECONDS,
        record.get("line"),
        record.get("route_id"),
        record.get("trip_id"),
        record.get("mode"),
        record.get("state_position"),
        _optional_int(record.get("delay")),
        _optional_int(record.get("actual_increase_60s")),
        _optional_float(record.get("delay_delta_1")),
        _optional_float(record.get("delay_mean_3")),
        _optional_float(record.get("speed")),
        _optional_float(record.get("speed_delta_1")),
        record.get("last_stop_id"),
        record.get("next_stop_id"),
        _optional_int(record.get("last_stop_sequence")),
        _optional_int(record.get("next_stop_sequence")),
        _optional_float(record.get("lat")),
        _optional_float(record.get("lon")),
    )


def _select_prediction_records_for_persistence(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    max_persisted = REALTIME_PREDICTION_MAX_PERSISTED_PER_RUN
    if max_persisted <= 0:
        return records

    alert_records = [
        record
        for record in records
        if record.get("delay_increase_alert")
    ]
    selected: list[dict[str, Any]] = list(alert_records)
    selected_ids = {id(record) for record in selected}
    remaining_slots = max(0, max_persisted - len(selected))

    if remaining_slots:
        for record in records:
            if id(record) in selected_ids:
                continue
            selected.append(record)
            selected_ids.add(id(record))
            remaining_slots -= 1
            if remaining_slots <= 0:
                break

    selected.sort(
        key=lambda item: item.get("delay_increase_risk", 0.0),
        reverse=True,
    )
    return selected


async def _persist_realtime_predictions(
    pool: asyncpg.Pool,
    records: list[dict[str, Any]],
    artifact: dict[str, Any],
) -> int:
    if not REALTIME_PREDICTION_PERSISTENCE_ENABLED or not records:
        return 0

    selected_records = _select_prediction_records_for_persistence(records)
    insert_records = [
        insert_tuple
        for insert_tuple in (
            _prediction_insert_tuple(record, artifact) for record in selected_records
        )
        if insert_tuple is not None
    ]
    if not insert_records:
        return 0

    retention_hours = max(1, REALTIME_PREDICTION_RETENTION_HOURS)
    try:
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                "delay_increase_predictions",
                records=insert_records,
                columns=PREDICTION_INSERT_COLUMNS,
            )
            await conn.execute(
                """
                DELETE FROM delay_increase_predictions
                WHERE scored_at < now() - $1::interval
                """,
                timedelta(hours=retention_hours),
            )
    except Exception as exc:
        logger.warning("Failed to persist realtime delay predictions: %s", exc)
        return 0

    return len(insert_records)


def _set_realtime_alert_snapshot(
    records: list[dict[str, Any]],
    artifact: dict[str, Any],
    alert_policy: dict[str, Any] | None = None,
) -> None:
    global REALTIME_ALERT_ERROR, REALTIME_ALERT_SNAPSHOT

    REALTIME_ALERT_ERROR = None
    alert_policy = alert_policy or {}
    REALTIME_ALERT_SNAPSHOT = {
        "generated_at": datetime.now(UTC).isoformat(),
        "records": records,
        "model_path": artifact["path"],
        "threshold": artifact["threshold"],
        "context_minutes": REALTIME_INFERENCE_CONTEXT_MINUTES,
        "output_freshness_seconds": REALTIME_INFERENCE_OUTPUT_FRESHNESS_SECONDS,
        "sampling_seconds": REALTIME_INFERENCE_SAMPLING_SECONDS,
        "alert_max_per_run": alert_policy.get("alert_max_per_run"),
        "alert_min_risk": alert_policy.get("alert_min_risk"),
        "raw_alert_count": alert_policy.get("raw_alert_count"),
        "eligible_alert_count": alert_policy.get("eligible_alert_count"),
        "suppressed_alert_count": alert_policy.get("suppressed_alert_count"),
        "persisted_prediction_count": alert_policy.get("persisted_prediction_count"),
        "prediction_retention_hours": REALTIME_PREDICTION_RETENTION_HOURS,
    }


def _set_realtime_alert_error(message: str) -> None:
    global REALTIME_ALERT_ERROR

    if message != REALTIME_ALERT_ERROR:
        logger.warning("Realtime delay inference unavailable: %s", message)
    REALTIME_ALERT_ERROR = message


def _read_realtime_delay_alert_records(alerts_only: bool) -> list[dict[str, Any]] | None:
    if REALTIME_ALERT_SNAPSHOT is None:
        return None

    records = [
        record.copy()
        for record in REALTIME_ALERT_SNAPSHOT.get("records", [])
        if isinstance(record, dict)
    ]
    if alerts_only:
        records = [
            record
            for record in records
            if record.get("delay_increase_alert")
        ]
    return records


def _coerce_alert_record(record: Any) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return None

    risk = record.get("delay_increase_risk")
    try:
        risk_value = float(risk)
    except (TypeError, ValueError):
        risk_value = 0.0

    return {
        **record,
        "delay_increase_risk": risk_value,
        "delay_increase_alert": bool(record.get("delay_increase_alert")),
    }


def _read_delay_alert_records(alerts_only: bool) -> tuple[Path, bool, list[dict[str, Any]]]:
    alerts_path = _get_delay_alerts_path()
    if not alerts_path.exists():
        return alerts_path, False, []

    raw_records = json.loads(alerts_path.read_text(encoding="utf-8"))
    if not isinstance(raw_records, list):
        raise TypeError("Delay alert artifact must be a JSON array.")

    records = [
        record
        for record in (_coerce_alert_record(raw_record) for raw_record in raw_records)
        if record is not None
    ]
    if alerts_only:
        records = [
            record
            for record in records
            if record.get("delay_increase_alert")
        ]
    return alerts_path, True, records


def _realtime_delay_alert_quality(now: datetime) -> dict[str, Any] | None:
    if REALTIME_ALERT_SNAPSHOT is None:
        if REALTIME_ALERT_ERROR:
            return {
                "status": "error",
                "reason": REALTIME_ALERT_ERROR,
                "artifact_path": "realtime://vehicle_positions",
                "artifact_exists": False,
                "artifact_mtime": None,
                "artifact_age_seconds": None,
                "latest_alert_time": None,
                "latest_alert_age_seconds": None,
                "total_count": 0,
                "alert_count": 0,
                "source": "realtime",
            }
        return None

    records = [
        record
        for record in REALTIME_ALERT_SNAPSHOT.get("records", [])
        if isinstance(record, dict)
    ]
    alert_records = [
        record
        for record in records
        if record.get("delay_increase_alert")
    ]
    generated_at = _parse_datetime(REALTIME_ALERT_SNAPSHOT.get("generated_at"))
    generated_age_seconds = _age_seconds(generated_at, now)
    parsed_times = [
        parsed_time
        for parsed_time in (_parse_datetime(record.get("time")) for record in records)
        if parsed_time is not None
    ]
    latest_alert_time = max(parsed_times) if parsed_times else None
    latest_alert_age_seconds = _age_seconds(latest_alert_time, now)

    status = "fresh"
    reason = "Realtime delay inference has scored the latest vehicle context."
    if not records:
        status = "missing"
        reason = "Realtime delay inference ran but did not produce any scorable rows."
    elif generated_age_seconds is None:
        status = "stale"
        reason = "Realtime delay inference has no generated_at timestamp."
    elif generated_age_seconds > REALTIME_INFERENCE_STALE_SECONDS:
        status = "stale"
        reason = (
            f"Realtime delay inference is {round(generated_age_seconds)}s old; "
            f"threshold is {REALTIME_INFERENCE_STALE_SECONDS}s."
        )
    elif latest_alert_age_seconds is None:
        status = "stale"
        reason = "Realtime delay rows do not include a valid time column."

    return {
        "status": status,
        "reason": reason,
        "artifact_path": "realtime://vehicle_positions",
        "artifact_exists": True,
        "artifact_mtime": generated_at.isoformat() if generated_at else None,
        "artifact_age_seconds": generated_age_seconds,
        "latest_alert_time": latest_alert_time.isoformat() if latest_alert_time else None,
        "latest_alert_age_seconds": latest_alert_age_seconds,
        "total_count": len(records),
        "alert_count": len(alert_records),
        "source": "realtime",
        "model_path": REALTIME_ALERT_SNAPSHOT.get("model_path"),
        "threshold": REALTIME_ALERT_SNAPSHOT.get("threshold"),
        "context_minutes": REALTIME_ALERT_SNAPSHOT.get("context_minutes"),
        "output_freshness_seconds": REALTIME_ALERT_SNAPSHOT.get(
            "output_freshness_seconds"
        ),
        "sampling_seconds": REALTIME_ALERT_SNAPSHOT.get("sampling_seconds"),
        "alert_max_per_run": REALTIME_ALERT_SNAPSHOT.get("alert_max_per_run"),
        "alert_min_risk": REALTIME_ALERT_SNAPSHOT.get("alert_min_risk"),
        "raw_alert_count": REALTIME_ALERT_SNAPSHOT.get("raw_alert_count"),
        "eligible_alert_count": REALTIME_ALERT_SNAPSHOT.get("eligible_alert_count"),
        "suppressed_alert_count": REALTIME_ALERT_SNAPSHOT.get(
            "suppressed_alert_count"
        ),
        "persisted_prediction_count": REALTIME_ALERT_SNAPSHOT.get(
            "persisted_prediction_count"
        ),
        "prediction_retention_hours": REALTIME_ALERT_SNAPSHOT.get(
            "prediction_retention_hours"
        ),
    }


def _delay_alert_quality(now: datetime, prefer_realtime: bool = True) -> dict[str, Any]:
    if prefer_realtime:
        realtime_quality = _realtime_delay_alert_quality(now)
        if realtime_quality is not None:
            return realtime_quality

    alerts_path = _get_delay_alerts_path()
    if not alerts_path.exists():
        return {
            "status": "missing",
            "reason": "Delay alert artifact does not exist.",
            "artifact_path": str(alerts_path),
            "artifact_exists": False,
            "artifact_mtime": None,
            "artifact_age_seconds": None,
            "latest_alert_time": None,
            "latest_alert_age_seconds": None,
            "total_count": 0,
            "alert_count": 0,
            "source": "artifact",
        }

    try:
        raw_records = json.loads(alerts_path.read_text(encoding="utf-8"))
        if not isinstance(raw_records, list):
            raise TypeError("Delay alert artifact must be a JSON array.")
    except (json.JSONDecodeError, TypeError) as exc:
        return {
            "status": "error",
            "reason": str(exc),
            "artifact_path": str(alerts_path),
            "artifact_exists": True,
            "artifact_mtime": datetime.fromtimestamp(
                alerts_path.stat().st_mtime,
                UTC,
            ).isoformat(),
            "artifact_age_seconds": _age_seconds(
                datetime.fromtimestamp(alerts_path.stat().st_mtime, UTC),
                now,
            ),
            "latest_alert_time": None,
            "latest_alert_age_seconds": None,
            "total_count": 0,
            "alert_count": 0,
            "source": "artifact",
        }

    records = [
        record
        for record in (_coerce_alert_record(raw_record) for raw_record in raw_records)
        if record is not None
    ]
    alert_records = [
        record
        for record in records
        if record.get("delay_increase_alert")
    ]
    parsed_times = [
        parsed_time
        for parsed_time in (_parse_datetime(record.get("time")) for record in records)
        if parsed_time is not None
    ]
    latest_alert_time = max(parsed_times) if parsed_times else None
    latest_alert_age_seconds = _age_seconds(latest_alert_time, now)
    artifact_mtime = datetime.fromtimestamp(alerts_path.stat().st_mtime, UTC)
    artifact_age_seconds = _age_seconds(artifact_mtime, now)

    status = "fresh"
    reason = "Alert artifact and scored rows are within freshness thresholds."
    if not records:
        status = "missing"
        reason = "Delay alert artifact exists but contains no valid rows."
    elif artifact_age_seconds is not None and artifact_age_seconds > ALERT_ARTIFACT_FRESHNESS_SECONDS:
        status = "stale"
        reason = (
            f"Alert artifact is {round(artifact_age_seconds)}s old; "
            f"threshold is {ALERT_ARTIFACT_FRESHNESS_SECONDS}s."
        )
    elif latest_alert_age_seconds is None:
        status = "stale"
        reason = "Alert rows do not include a valid time column."
    elif latest_alert_age_seconds > ALERT_ROW_FRESHNESS_SECONDS:
        status = "stale"
        reason = (
            f"Latest scored row is {round(latest_alert_age_seconds)}s old; "
            f"threshold is {ALERT_ROW_FRESHNESS_SECONDS}s."
        )

    return {
        "status": status,
        "reason": reason,
        "artifact_path": str(alerts_path),
        "artifact_exists": True,
        "artifact_mtime": artifact_mtime.isoformat(),
        "artifact_age_seconds": artifact_age_seconds,
        "latest_alert_time": latest_alert_time.isoformat() if latest_alert_time else None,
        "latest_alert_age_seconds": latest_alert_age_seconds,
        "total_count": len(records),
        "alert_count": len(alert_records),
        "source": "artifact",
    }


def _pipeline_report_quality(now: datetime) -> dict[str, Any]:
    report_path = _get_pipeline_quality_report_path()
    if not report_path.exists():
        return {
            "status": "missing",
            "reason": "Pipeline quality report does not exist.",
            "report_path": str(report_path),
            "report_exists": False,
            "report_mtime": None,
            "generated_at": None,
            "report_age_seconds": None,
            "freshness_threshold_seconds": PIPELINE_REPORT_FRESHNESS_SECONDS,
            "summary": {"failed": 0, "warnings": 0, "passed": 0},
            "checks": [],
        }

    report_mtime = datetime.fromtimestamp(report_path.stat().st_mtime, UTC)
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if not isinstance(report, dict):
            raise TypeError("Pipeline quality report must be a JSON object.")
    except (json.JSONDecodeError, TypeError) as exc:
        return {
            "status": "error",
            "reason": str(exc),
            "report_path": str(report_path),
            "report_exists": True,
            "report_mtime": report_mtime.isoformat(),
            "generated_at": None,
            "report_age_seconds": _age_seconds(report_mtime, now),
            "freshness_threshold_seconds": PIPELINE_REPORT_FRESHNESS_SECONDS,
            "summary": {"failed": 0, "warnings": 0, "passed": 0},
            "checks": [],
        }

    raw_summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    summary = {
        "failed": _safe_int(raw_summary.get("failed")),
        "warnings": _safe_int(raw_summary.get("warnings")),
        "passed": _safe_int(raw_summary.get("passed")),
    }
    generated_at = _parse_datetime(report.get("generated_at"))
    report_age_seconds = _age_seconds(generated_at, now)
    checks = []
    for check in report.get("checks") if isinstance(report.get("checks"), list) else []:
        if not isinstance(check, dict):
            continue
        status = str(check.get("status") or "")
        if status not in {"failed", "warning"}:
            continue
        checks.append(
            {
                "name": str(check.get("name") or "unnamed_check"),
                "status": status,
                "message": str(check.get("message") or ""),
            }
        )

    report_status = str(report.get("status") or "")
    if report_status == "failed" or summary["failed"] > 0:
        status = "error"
        reason = f"Pipeline quality validation failed: {summary['failed']} failed check(s)."
    elif generated_at is None:
        status = "stale"
        reason = "Pipeline quality report has no parseable generated_at timestamp."
    elif (
        report_age_seconds is not None
        and report_age_seconds > PIPELINE_REPORT_FRESHNESS_SECONDS
    ):
        status = "stale"
        reason = (
            f"Pipeline quality report is {round(report_age_seconds)}s old; "
            f"threshold is {PIPELINE_REPORT_FRESHNESS_SECONDS}s."
        )
    elif summary["warnings"] > 0:
        status = "stale"
        reason = f"Pipeline quality validation passed with {summary['warnings']} warning(s)."
    else:
        status = "fresh"
        reason = "Latest pipeline quality report passed."

    return {
        "status": status,
        "reason": reason,
        "report_path": str(report_path),
        "report_exists": True,
        "report_mtime": report_mtime.isoformat(),
        "generated_at": generated_at.isoformat() if generated_at else None,
        "report_age_seconds": report_age_seconds,
        "freshness_threshold_seconds": PIPELINE_REPORT_FRESHNESS_SECONDS,
        "summary": summary,
        "checks": checks[:5],
    }


async def _table_quality(
    conn: asyncpg.Connection,
    table_name: str,
    now: datetime,
    freshness_seconds: int,
) -> dict[str, Any]:
    if table_name not in {"vehicle_positions", "vehicle_trajectory_points"}:
        raise ValueError(f"Unsupported quality table: {table_name}")

    latest_time = await conn.fetchval(f"SELECT max(time) FROM {table_name}")
    latest_time = _parse_datetime(latest_time)
    latest_window_start = (
        latest_time - timedelta(seconds=QUALITY_WINDOW_SECONDS)
        if latest_time is not None
        else now
    )
    recent_window_start = now - timedelta(seconds=QUALITY_WINDOW_SECONDS)

    row = await conn.fetchrow(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE time >= $1) AS rows_near_latest,
            COUNT(DISTINCT vehicle_id) FILTER (WHERE time >= $1) AS vehicles_near_latest,
            COUNT(*) FILTER (WHERE time >= $2) AS rows_recent,
            COUNT(DISTINCT vehicle_id) FILTER (WHERE time >= $2) AS vehicles_recent
        FROM {table_name}
        """,
        latest_window_start,
        recent_window_start,
    )
    rows_near_latest = int(row["rows_near_latest"] or 0) if row else 0
    vehicles_near_latest = int(row["vehicles_near_latest"] or 0) if row else 0
    rows_recent = int(row["rows_recent"] or 0) if row else 0
    vehicles_recent = int(row["vehicles_recent"] or 0) if row else 0
    age = _age_seconds(latest_time, now)
    status, reason = _source_status(
        latest_time,
        age,
        freshness_seconds,
        rows_near_latest,
    )

    return {
        "status": status,
        "reason": reason,
        "latest_time": latest_time.isoformat() if latest_time else None,
        "latest_age_seconds": age,
        "freshness_threshold_seconds": freshness_seconds,
        "window_seconds": QUALITY_WINDOW_SECONDS,
        "rows_near_latest": rows_near_latest,
        "vehicles_near_latest": vehicles_near_latest,
        "rows_recent": rows_recent,
        "vehicles_recent": vehicles_recent,
    }


@app.get("/delay-increase-alerts")
async def delay_increase_alerts(
    limit: int = Query(100, ge=1, le=1000),
    alerts_only: bool = True,
) -> dict[str, Any]:
    """Return the latest scored delay-increase risk rows from a JSON artifact."""

    now = datetime.now(UTC)
    realtime_records = _read_realtime_delay_alert_records(alerts_only)
    if realtime_records is not None:
        realtime_records.sort(
            key=lambda record: record.get("delay_increase_risk", 0.0),
            reverse=True,
        )
        page = realtime_records[:limit]
        alert_quality = _delay_alert_quality(now)
        return {
            "meta": {
                "artifact_path": alert_quality["artifact_path"],
                "artifact_exists": alert_quality["artifact_exists"],
                "artifact_mtime": alert_quality["artifact_mtime"],
                "artifact_age_seconds": alert_quality["artifact_age_seconds"],
                "latest_alert_time": alert_quality["latest_alert_time"],
                "latest_alert_age_seconds": alert_quality["latest_alert_age_seconds"],
                "status": alert_quality["status"],
                "reason": alert_quality["reason"],
                "total_count": len(realtime_records),
                "scored_count": alert_quality["total_count"],
                "alert_count": alert_quality["alert_count"],
                "returned_count": len(page),
                "alerts_only": alerts_only,
                "source": alert_quality.get("source"),
                "model_path": alert_quality.get("model_path"),
                "threshold": alert_quality.get("threshold"),
                "context_minutes": alert_quality.get("context_minutes"),
                "output_freshness_seconds": alert_quality.get(
                    "output_freshness_seconds"
                ),
                "sampling_seconds": alert_quality.get("sampling_seconds"),
                "alert_max_per_run": alert_quality.get("alert_max_per_run"),
                "alert_min_risk": alert_quality.get("alert_min_risk"),
                "raw_alert_count": alert_quality.get("raw_alert_count"),
                "eligible_alert_count": alert_quality.get("eligible_alert_count"),
                "suppressed_alert_count": alert_quality.get(
                    "suppressed_alert_count"
                ),
                "persisted_prediction_count": alert_quality.get(
                    "persisted_prediction_count"
                ),
                "prediction_retention_hours": alert_quality.get(
                    "prediction_retention_hours"
                ),
            },
            "data": page,
        }

    try:
        alerts_path, artifact_exists, records = _read_delay_alert_records(alerts_only)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Delay alert artifact is invalid JSON: {exc}",
        ) from exc
    except TypeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not artifact_exists:
        return {
            "meta": {
                "artifact_path": str(alerts_path),
                "artifact_mtime": None,
                "artifact_exists": False,
                "artifact_age_seconds": None,
                "latest_alert_time": None,
                "latest_alert_age_seconds": None,
                "status": "missing",
                "reason": "Delay alert artifact does not exist.",
                "total_count": 0,
                "scored_count": 0,
                "alert_count": 0,
                "returned_count": 0,
                "alerts_only": alerts_only,
                "source": "artifact",
            },
            "data": [],
        }

    records.sort(
        key=lambda record: record.get("delay_increase_risk", 0.0),
        reverse=True,
    )
    page = records[:limit]
    alert_quality = _delay_alert_quality(now, prefer_realtime=False)

    return {
        "meta": {
            "artifact_path": str(alerts_path),
            "artifact_exists": True,
            "artifact_mtime": alert_quality["artifact_mtime"],
            "artifact_age_seconds": alert_quality["artifact_age_seconds"],
            "latest_alert_time": alert_quality["latest_alert_time"],
            "latest_alert_age_seconds": alert_quality["latest_alert_age_seconds"],
            "status": alert_quality["status"],
            "reason": alert_quality["reason"],
            "total_count": len(records),
            "scored_count": alert_quality["total_count"],
            "alert_count": alert_quality["alert_count"],
            "returned_count": len(page),
            "alerts_only": alerts_only,
            "source": alert_quality.get("source"),
        },
        "data": page,
    }


@app.get("/data-quality")
async def data_quality(request: Request) -> dict[str, Any]:
    """Return freshness and row-count checks for the dashboard data sources."""

    db_pool = getattr(request.app.state, "db_pool", None)
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    now = datetime.now(UTC)
    query_started = perf_counter()
    async with db_pool.acquire() as conn:
        live_quality = await _table_quality(
            conn,
            "vehicle_positions",
            now,
            LIVE_FRESHNESS_SECONDS,
        )
        replay_quality = await _table_quality(
            conn,
            "vehicle_trajectory_points",
            now,
            REPLAY_FRESHNESS_SECONDS,
        )
    alert_quality = _delay_alert_quality(now)
    pipeline_quality = _pipeline_report_quality(now)
    elapsed_ms = round((perf_counter() - query_started) * 1000.0, 2)
    sources = {
        "live_positions": live_quality,
        "replay_trajectory": replay_quality,
        "delay_alerts": alert_quality,
        "pipeline_quality": pipeline_quality,
    }

    return {
        "meta": {
            "generated_at": now.isoformat(),
            "query_time_ms": elapsed_ms,
            "thresholds_seconds": {
                "live": LIVE_FRESHNESS_SECONDS,
                "replay": REPLAY_FRESHNESS_SECONDS,
                "alert_artifact": ALERT_ARTIFACT_FRESHNESS_SECONDS,
                "alert_rows": ALERT_ROW_FRESHNESS_SECONDS,
                "pipeline_report": PIPELINE_REPORT_FRESHNESS_SECONDS,
                "quality_window": QUALITY_WINDOW_SECONDS,
            },
        },
        "overall_status": _overall_quality_status(
            [source["status"] for source in sources.values()]
        ),
        "sources": sources,
    }


@app.get("/replay")
async def replay(
    request: Request,
    start_ts: datetime,
    end_ts: datetime,
    vehicle_id: str | None = None,
    mode: str | None = None,
    limit: int = Query(5000, ge=1, le=20000),
    cursor_time: datetime | None = None,
    cursor_vehicle_id: str | None = None,
):
    """Return a stable, cursor-paginated replay slice for the requested time window."""

    if end_ts <= start_ts:
        raise HTTPException(status_code=400, detail="end_ts must be greater than start_ts")

    if (cursor_time is None) != (cursor_vehicle_id is None):
        raise HTTPException(
            status_code=400,
            detail="cursor_time and cursor_vehicle_id must be provided together",
        )

    rows_sql = """
        SELECT
            time,
            vehicle_id,
            lat,
            lon,
            point_state,
            confidence,
            interpolation_method,
            gap_reason,
            route_id,
            trip_id,
            mode
        FROM vehicle_trajectory_points
        WHERE time >= $1 AND time < $2
            AND ($3::text IS NULL OR vehicle_id = $3)
            AND ($4::text IS NULL OR mode = $4)
            AND (
                $5::timestamptz IS NULL
                OR (time, vehicle_id) > ($5, $6)
            )
        ORDER BY time ASC, vehicle_id ASC
        LIMIT $7
        """

    count_sql = """
        SELECT COUNT(*)
        FROM vehicle_trajectory_points
        WHERE time >= $1 AND time < $2
            AND ($3::text IS NULL OR vehicle_id = $3)
            AND ($4::text IS NULL OR mode = $4)
        """

    db_pool = getattr(request.app.state, "db_pool", None)
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    query_started = perf_counter()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            rows_sql,
            start_ts,
            end_ts,
            vehicle_id,
            mode,
            cursor_time,
            cursor_vehicle_id,
            limit + 1,
        )
        total_count = None
        if cursor_time is None:
            total_count = await conn.fetchval(
                count_sql,
                start_ts,
                end_ts,
                vehicle_id,
                mode,
            )
    elapsed_ms = round((perf_counter() - query_started) * 1000.0, 2)

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    data = [dict(r) for r in page_rows]

    next_cursor_time = None
    next_cursor_vehicle_id = None
    if has_more and page_rows:
        last_row = page_rows[-1]
        next_cursor_time = last_row["time"].isoformat()
        next_cursor_vehicle_id = last_row["vehicle_id"]

    return {
        "meta": {
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "vehicle_id": vehicle_id,
            "mode": mode,
            "requested_limit": limit,
            "returned_count": len(data),
            "total_count": int(total_count) if total_count is not None else None,
            "has_more": has_more,
            "query_time_ms": elapsed_ms,
            "next_cursor_time": next_cursor_time,
            "next_cursor_vehicle_id": next_cursor_vehicle_id,
        },
        "data": data,
    }


@app.get("/replay/bounds")
async def replay_bounds(request: Request) -> dict[str, Any]:
    """Return the available timestamp bounds for replay data."""

    db_pool = getattr(request.app.state, "db_pool", None)
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    query_started = perf_counter()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                min(time) AS min_time,
                max(time) AS max_time
            FROM vehicle_trajectory_points
            """,
        )
    elapsed_ms = round((perf_counter() - query_started) * 1000.0, 2)

    min_time = row["min_time"] if row else None
    max_time = row["max_time"] if row else None

    return {
        "meta": {
            "min_time": min_time.isoformat() if min_time else None,
            "max_time": max_time.isoformat() if max_time else None,
            "query_time_ms": elapsed_ms,
        },
    }
