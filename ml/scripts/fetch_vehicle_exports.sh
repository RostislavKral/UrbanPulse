#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ML_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

REMOTE_USER="${REMOTE_USER:-}"
REMOTE_HOST="${REMOTE_HOST:-}"
REMOTE_DIR="${REMOTE_DIR:-/opt/urban-pulse/ml/exports}"
REMOTE_EXPORT_BEFORE_FETCH="${REMOTE_EXPORT_BEFORE_FETCH:-false}"
REMOTE_EXPORT_COMMAND="${REMOTE_EXPORT_COMMAND:-}"
REMOTE_REPO_DIR="${REMOTE_REPO_DIR:-/opt/urban-pulse}"
REMOTE_EXPORT_SCRIPT="${REMOTE_EXPORT_SCRIPT:-export_vehicle_positions.sh}"
REMOTE_INCREMENTAL_EXPORT="${REMOTE_INCREMENTAL_EXPORT:-false}"
REMOTE_INCREMENTAL_MANIFEST="${REMOTE_INCREMENTAL_MANIFEST:-${ML_ROOT}/lake/vehicle_positions_manifest.json}"
REMOTE_INCREMENTAL_LOOKBACK_HOURS="${REMOTE_INCREMENTAL_LOOKBACK_HOURS:-2}"
REMOTE_INCREMENTAL_BOOTSTRAP_DAYS="${REMOTE_INCREMENTAL_BOOTSTRAP_DAYS:-3}"
REMOTE_INCREMENTAL_MAX_DAYS="${REMOTE_INCREMENTAL_MAX_DAYS:-3}"
REMOTE_EXPORT_DAYS="${REMOTE_EXPORT_DAYS:-35}"
REMOTE_EXPORT_START_TS="${REMOTE_EXPORT_START_TS:-}"
REMOTE_EXPORT_END_TS="${REMOTE_EXPORT_END_TS:-}"
REMOTE_EXPORT_SERIES_START_TS="${REMOTE_EXPORT_SERIES_START_TS:-}"
REMOTE_EXPORT_SERIES_END_TS="${REMOTE_EXPORT_SERIES_END_TS:-}"
REMOTE_EXPORT_CHUNK_HOURS="${REMOTE_EXPORT_CHUNK_HOURS:-24}"
REMOTE_EXPORT_SKIP_EXISTING="${REMOTE_EXPORT_SKIP_EXISTING:-false}"
REMOTE_EXPORT_OUTPUT_DIR="${REMOTE_EXPORT_OUTPUT_DIR:-${REMOTE_DIR}}"
REMOTE_CONTAINER_NAME="${REMOTE_CONTAINER_NAME:-${CONTAINER_NAME:-prague_db}}"
REMOTE_DB_USER="${REMOTE_DB_USER:-${DB_USER:-${POSTGRES_USER:-admin}}}"
REMOTE_DB_NAME="${REMOTE_DB_NAME:-${DB_NAME:-${POSTGRES_DB:-prague_transport}}}"
REMOTE_DB_PASSWORD="${REMOTE_DB_PASSWORD:-${DB_PASSWORD:-${POSTGRES_PASSWORD:-}}}"
DOWNLOAD_DIR="${DOWNLOAD_DIR:-${ML_ROOT}}"
ARCHIVE_PATH="${ARCHIVE_PATH:-${ML_ROOT}/vehicle_positions_all_chunks.tar.gz}"
CLEAN_DOWNLOAD_DIR_BEFORE_FETCH="${CLEAN_DOWNLOAD_DIR_BEFORE_FETCH:-true}"
CLEAN_REMOTE_DIR_BEFORE_EXPORT="${CLEAN_REMOTE_DIR_BEFORE_EXPORT:-false}"
CLEAN_REMOTE_DIR_AFTER_FETCH="${CLEAN_REMOTE_DIR_AFTER_FETCH:-false}"
CREATE_ARCHIVE="${CREATE_ARCHIVE:-true}"
SSH_OPTS="${SSH_OPTS:-}"
SCP_OPTS="${SCP_OPTS:-$SSH_OPTS}"

REMOTE_PATTERN="${REMOTE_DIR%/}/vehicle_positions_*.csv.gz"
TMP_ARCHIVE="${ARCHIVE_PATH}.tmp"

cleanup() {
  rm -f "$TMP_ARCHIVE"
}
trap cleanup EXIT

is_truthy() {
  case "${1,,}" in
    1 | true | yes | y | on | enabled)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

add_remote_env() {
  local -n target="$1"
  local name="$2"
  local value="$3"

  if [[ -n "$value" ]]; then
    target+=("${name}=$(printf "%q" "$value")")
  fi
}

require_remote_config() {
  if [[ -z "$REMOTE_USER" || -z "$REMOTE_HOST" ]]; then
    echo "REMOTE_USER and REMOTE_HOST must be set before fetching vehicle exports." >&2
    exit 1
  fi
}

configure_incremental_window() {
  local window

  if ! is_truthy "$REMOTE_INCREMENTAL_EXPORT"; then
    return
  fi
  if [[ -n "$REMOTE_EXPORT_START_TS" || -n "$REMOTE_EXPORT_END_TS" ]]; then
    echo "Using explicit remote export window."
    return
  fi
  if [[ -n "$REMOTE_EXPORT_SERIES_START_TS" || -n "$REMOTE_EXPORT_SERIES_END_TS" ]]; then
    echo "Using explicit remote export series window."
    return
  fi

  mapfile -t window < <(
    REMOTE_INCREMENTAL_MANIFEST="$REMOTE_INCREMENTAL_MANIFEST" \
    REMOTE_INCREMENTAL_LOOKBACK_HOURS="$REMOTE_INCREMENTAL_LOOKBACK_HOURS" \
    REMOTE_INCREMENTAL_BOOTSTRAP_DAYS="$REMOTE_INCREMENTAL_BOOTSTRAP_DAYS" \
    REMOTE_INCREMENTAL_MAX_DAYS="$REMOTE_INCREMENTAL_MAX_DAYS" \
      python - <<'PY'
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path


def parse_time(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    if " " in normalized and "T" not in normalized:
        normalized = normalized.replace(" ", "T", 1)
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


manifest_path = Path(os.environ["REMOTE_INCREMENTAL_MANIFEST"])
lookback = timedelta(hours=float(os.environ["REMOTE_INCREMENTAL_LOOKBACK_HOURS"]))
bootstrap = timedelta(days=float(os.environ["REMOTE_INCREMENTAL_BOOTSTRAP_DAYS"]))
max_window = timedelta(days=float(os.environ["REMOTE_INCREMENTAL_MAX_DAYS"]))
end = datetime.now(timezone.utc).replace(microsecond=0)

start = None
if manifest_path.exists():
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    max_time = manifest.get("max_time")
    if max_time:
        start = parse_time(str(max_time)) - lookback

if start is None:
    start = end - bootstrap

earliest_start = end - max_window
if start < earliest_start:
    start = earliest_start

if start >= end:
    start = end - lookback

fmt = "%Y-%m-%d %H:%M:%S+00"
print(start.strftime(fmt))
print(end.strftime(fmt))
PY
  )

  REMOTE_EXPORT_START_TS="${window[0]}"
  REMOTE_EXPORT_END_TS="${window[1]}"
  echo "Incremental remote export window: ${REMOTE_EXPORT_START_TS} -> ${REMOTE_EXPORT_END_TS}"
}

clean_remote_exports() {
  local ssh_args

  read -r -a ssh_args <<< "$SSH_OPTS"
  ssh "${ssh_args[@]}" "${REMOTE_USER}@${REMOTE_HOST}" \
    "mkdir -p $(printf "%q" "$REMOTE_EXPORT_OUTPUT_DIR") && find $(printf "%q" "$REMOTE_EXPORT_OUTPUT_DIR") -maxdepth 1 -type f -name 'vehicle_positions_*.csv.gz' -delete"
}

run_remote_export() {
  local ssh_args
  local remote_command
  local remote_script_path
  local remote_env=()

  read -r -a ssh_args <<< "$SSH_OPTS"

  if [[ -n "$REMOTE_EXPORT_COMMAND" ]]; then
    remote_command="$REMOTE_EXPORT_COMMAND"
  else
    if [[ "$REMOTE_EXPORT_SCRIPT" = /* ]]; then
      remote_script_path="$REMOTE_EXPORT_SCRIPT"
    else
      remote_script_path="./${REMOTE_EXPORT_SCRIPT#./}"
    fi
    add_remote_env remote_env "EXPORT_DAYS" "$REMOTE_EXPORT_DAYS"
    add_remote_env remote_env "EXPORT_START_TS" "$REMOTE_EXPORT_START_TS"
    add_remote_env remote_env "EXPORT_END_TS" "$REMOTE_EXPORT_END_TS"
    add_remote_env remote_env "EXPORT_SERIES_START_TS" "$REMOTE_EXPORT_SERIES_START_TS"
    add_remote_env remote_env "EXPORT_SERIES_END_TS" "$REMOTE_EXPORT_SERIES_END_TS"
    add_remote_env remote_env "EXPORT_CHUNK_HOURS" "$REMOTE_EXPORT_CHUNK_HOURS"
    add_remote_env remote_env "SKIP_EXISTING" "$REMOTE_EXPORT_SKIP_EXISTING"
    add_remote_env remote_env "OUTPUT_DIR" "$REMOTE_EXPORT_OUTPUT_DIR"
    add_remote_env remote_env "CONTAINER_NAME" "$REMOTE_CONTAINER_NAME"
    add_remote_env remote_env "DB_USER" "$REMOTE_DB_USER"
    add_remote_env remote_env "DB_NAME" "$REMOTE_DB_NAME"
    add_remote_env remote_env "DB_PASSWORD" "$REMOTE_DB_PASSWORD"

    remote_command="cd $(printf "%q" "$REMOTE_REPO_DIR") && "
    if [[ "${#remote_env[@]}" -gt 0 ]]; then
      remote_command+="${remote_env[*]} "
    fi
    remote_command+="$(printf "%q" "$remote_script_path")"
  fi

  if is_truthy "$CLEAN_REMOTE_DIR_BEFORE_EXPORT"; then
    echo "Cleaning remote vehicle export chunks in ${REMOTE_EXPORT_OUTPUT_DIR}"
    clean_remote_exports
  fi

  echo "Running remote export on ${REMOTE_USER}@${REMOTE_HOST}"
  ssh "${ssh_args[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "$remote_command"
}

mkdir -p "$DOWNLOAD_DIR"

configure_incremental_window
require_remote_config

if is_truthy "$REMOTE_EXPORT_BEFORE_FETCH"; then
  run_remote_export
fi

if is_truthy "$CLEAN_DOWNLOAD_DIR_BEFORE_FETCH"; then
  echo "Cleaning existing local vehicle export chunks in ${DOWNLOAD_DIR}"
  rm -f "${DOWNLOAD_DIR}"/vehicle_positions_*.csv.gz
fi

read -r -a scp_args <<< "$SCP_OPTS"

echo "Fetching ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATTERN}"
echo "Destination: ${DOWNLOAD_DIR}"
scp "${scp_args[@]}" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATTERN}" "${DOWNLOAD_DIR}/"

if is_truthy "$CLEAN_REMOTE_DIR_AFTER_FETCH"; then
  echo "Cleaning remote vehicle export chunks after fetch."
  clean_remote_exports
fi

mapfile -d '' CHUNKS < <(
  find "$DOWNLOAD_DIR" \
    -maxdepth 1 \
    -type f \
    -name 'vehicle_positions_*.csv.gz' \
    -printf '%f\0' \
    | sort -z
)

if [[ "${#CHUNKS[@]}" -eq 0 ]]; then
  echo "No vehicle_positions_*.csv.gz files found in ${DOWNLOAD_DIR}" >&2
  exit 1
fi

if is_truthy "$CREATE_ARCHIVE"; then
  rm -f "$TMP_ARCHIVE"

  echo "Packing ${#CHUNKS[@]} chunk(s) -> ${ARCHIVE_PATH}"
  printf '%s\0' "${CHUNKS[@]}" \
    | tar --null -czf "$TMP_ARCHIVE" -C "$DOWNLOAD_DIR" -T -
  mv "$TMP_ARCHIVE" "$ARCHIVE_PATH"
  trap - EXIT

  echo "Done."
  ls -lh "$ARCHIVE_PATH"
else
  trap - EXIT
  echo "Fetched ${#CHUNKS[@]} chunk(s). Archive creation skipped."
fi
