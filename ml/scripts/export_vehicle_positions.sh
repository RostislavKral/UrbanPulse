#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

CONTAINER_NAME="${CONTAINER_NAME:-prague_db}"
DB_USER="${DB_USER:-${POSTGRES_USER:-admin}}"
DB_NAME="${DB_NAME:-${POSTGRES_DB:-prague_transport}}"
DB_PASSWORD="${DB_PASSWORD:-${POSTGRES_PASSWORD:-}}"
EXPORT_DAYS="${EXPORT_DAYS:-35}"
EXPORT_START_TS="${EXPORT_START_TS:-}"
EXPORT_END_TS="${EXPORT_END_TS:-}"
EXPORT_SERIES_START_TS="${EXPORT_SERIES_START_TS:-}"
EXPORT_SERIES_END_TS="${EXPORT_SERIES_END_TS:-}"
EXPORT_CHUNK_HOURS="${EXPORT_CHUNK_HOURS:-24}"
SKIP_EXISTING="${SKIP_EXISTING:-true}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/ml/exports}"
ARCHIVE_SERIES="${ARCHIVE_SERIES:-false}"
ARCHIVE_NAME="${ARCHIVE_NAME:-vehicle_positions_series_$(date -u '+%Y%m%dT%H%M%SZ').tar.gz}"

if [[ -n "$EXPORT_SERIES_START_TS" || -n "$EXPORT_SERIES_END_TS" ]]; then
    if [[ -n "${1:-}" ]]; then
        echo "Do not pass OUTPUT_PATH when using EXPORT_SERIES_START_TS/EXPORT_SERIES_END_TS" >&2
        exit 1
    fi
    if [[ -z "$EXPORT_SERIES_START_TS" ]]; then
        echo "EXPORT_SERIES_START_TS must be provided for series export" >&2
        exit 1
    fi
    if [[ -z "$EXPORT_SERIES_END_TS" ]]; then
        EXPORT_SERIES_END_TS="$(date -u '+%Y-%m-%d %H:%M:%S+00')"
    fi
fi

if [[ -n "$EXPORT_START_TS" || -n "$EXPORT_END_TS" ]]; then
    if [[ -z "$EXPORT_START_TS" || -z "$EXPORT_END_TS" ]]; then
        echo "EXPORT_START_TS and EXPORT_END_TS must be provided together" >&2
        exit 1
    fi

    SAFE_START_TS="$(echo "$EXPORT_START_TS" | tr ': ' '__')"
    SAFE_END_TS="$(echo "$EXPORT_END_TS" | tr ': ' '__')"
    DEFAULT_BASENAME="vehicle_positions_${SAFE_START_TS}_to_${SAFE_END_TS}.csv.gz"
else
    DEFAULT_BASENAME="vehicle_positions_last_${EXPORT_DAYS}d.csv.gz"
fi

OUTPUT_PATH="${1:-$OUTPUT_DIR/$DEFAULT_BASENAME}"

mkdir -p "$OUTPUT_DIR"

build_sql() {
    local time_filter="$1"
    local sql

    read -r -d '' sql <<'EOF' || true
COPY (
    SELECT
        time,
        vehicle_id,
        line,
        delay,
        lat,
        lon,
        speed,
        route_id,
        mode,
        route_type,
        trip_id,
        state_position,
        origin_timestamp,
        last_stop_id,
        last_stop_sequence,
        last_stop_arrival_time,
        last_stop_departure_time,
        next_stop_id,
        next_stop_sequence,
        next_stop_arrival_time,
        next_stop_departure_time
    FROM vehicle_positions
    WHERE __TIME_FILTER__
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)
EOF

    echo "${sql/__TIME_FILTER__/$time_filter}"
}

run_export() {
    local time_filter="$1"
    local output_path="$2"
    local label="$3"
    local sql

    sql="$(build_sql "$time_filter")"

    echo "Exporting ${label} from ${CONTAINER_NAME}..."
    echo "Writing ${output_path}"

    docker exec -i \
        -e PGPASSWORD="${DB_PASSWORD}" \
        "$CONTAINER_NAME" \
        psql \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -c "$sql" \
        | gzip > "$output_path"

    echo "Done: ${output_path}"
}

if [[ -n "$EXPORT_SERIES_START_TS" ]]; then
    current_ts="$(date -u -d "$EXPORT_SERIES_START_TS" '+%Y-%m-%d %H:%M:%S+00')"
    series_end_ts="$(date -u -d "$EXPORT_SERIES_END_TS" '+%Y-%m-%d %H:%M:%S+00')"
    chunk_files=()

    while [[ "$(date -u -d "$current_ts" '+%s')" -lt "$(date -u -d "$series_end_ts" '+%s')" ]]; do
        next_ts="$(date -u -d "$current_ts + ${EXPORT_CHUNK_HOURS} hours" '+%Y-%m-%d %H:%M:%S+00')"
        if [[ "$(date -u -d "$next_ts" '+%s')" -gt "$(date -u -d "$series_end_ts" '+%s')" ]]; then
            next_ts="$series_end_ts"
        fi

        safe_start_ts="$(echo "$current_ts" | tr ': ' '__')"
        safe_end_ts="$(echo "$next_ts" | tr ': ' '__')"
        output_path="${OUTPUT_DIR}/vehicle_positions_${safe_start_ts}_to_${safe_end_ts}.csv.gz"
        chunk_files+=("$(basename "$output_path")")

        if [[ "$SKIP_EXISTING" == "true" && -f "$output_path" ]]; then
            echo "Skipping existing chunk: ${output_path}"
        else
            run_export \
                "time >= TIMESTAMPTZ '$current_ts' AND time < TIMESTAMPTZ '$next_ts'" \
                "$output_path" \
                "time window ${current_ts} -> ${next_ts}"
        fi

        current_ts="$next_ts"
    done

    if [[ "$ARCHIVE_SERIES" == "true" && "${#chunk_files[@]}" -gt 0 ]]; then
        archive_path="${OUTPUT_DIR}/${ARCHIVE_NAME}"
        echo "Creating archive ${archive_path}"
        tar -C "$OUTPUT_DIR" -czf "$archive_path" "${chunk_files[@]}"
        echo "Done: ${archive_path}"
    fi
else
    if [[ -n "$EXPORT_START_TS" ]]; then
        TIME_FILTER="time >= TIMESTAMPTZ '$EXPORT_START_TS' AND time < TIMESTAMPTZ '$EXPORT_END_TS'"
        LABEL="time window ${EXPORT_START_TS} -> ${EXPORT_END_TS}"
    else
        TIME_FILTER="time >= NOW() - INTERVAL '$EXPORT_DAYS days'"
        LABEL="last ${EXPORT_DAYS} days"
    fi

    run_export "$TIME_FILTER" "$OUTPUT_PATH" "$LABEL"
fi
