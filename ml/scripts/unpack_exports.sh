#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ARCHIVE_PATH="${1:-${ROOT_DIR}/vehicle_positions_all_chunks.tar.gz}"
RAW_DIR="${RAW_DIR:-${ROOT_DIR}/data/raw}"

mkdir -p "$RAW_DIR"

echo "Extracting ${ARCHIVE_PATH} -> ${RAW_DIR}"
tar -xzf "$ARCHIVE_PATH" -C "$RAW_DIR"
echo "Done: ${RAW_DIR}"
