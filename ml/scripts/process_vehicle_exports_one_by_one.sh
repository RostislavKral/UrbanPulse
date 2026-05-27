#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ML_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ML_ROOT}/.." && pwd)"

PYTHON="${PYTHON:-${ML_ROOT}/.venv/bin/python}"
INPUT_DIR="${INPUT_DIR:-${ML_ROOT}}"
INPUT_NAME_GLOB="${INPUT_NAME_GLOB:-vehicle_positions_2026-0[45]-*.csv.gz}"
OUTPUT_DIR="${OUTPUT_DIR:-${ML_ROOT}/data/processed}"
OUTPUT_PREFIX="${OUTPUT_PREFIX:-delay_baseline_5min_recent}"
POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-2}"
FORCE="${FORCE:-0}"
DRY_RUN="${DRY_RUN:-0}"
FAILED=()

export POLARS_MAX_THREADS

mkdir -p "$OUTPUT_DIR"

mapfile -d '' INPUT_FILES < <(
  find "$INPUT_DIR" \
    -maxdepth 1 \
    -type f \
    -name "$INPUT_NAME_GLOB" \
    -printf '%p\0' \
    | sort -z
)

if [[ "${#INPUT_FILES[@]}" -eq 0 ]]; then
  echo "No files matched ${INPUT_DIR}/${INPUT_NAME_GLOB}" >&2
  exit 1
fi

echo "Processing ${#INPUT_FILES[@]} file(s) one at a time."
echo "Input dir: ${INPUT_DIR}"
echo "Output dir: ${OUTPUT_DIR}"
echo "POLARS_MAX_THREADS=${POLARS_MAX_THREADS}"

for input_path in "${INPUT_FILES[@]}"; do
  input_name="$(basename "$input_path")"
  output_name="${input_name#vehicle_positions_}"
  output_name="${output_name%.csv.gz}"
  output_path="${OUTPUT_DIR}/${OUTPUT_PREFIX}_${output_name}.parquet"

  if [[ -f "$output_path" && "$FORCE" != "1" ]]; then
    echo "Skipping existing: ${output_path}"
    continue
  fi

  echo
  echo "Building: ${input_name}"
  echo " -> ${output_path}"

  if [[ "$DRY_RUN" == "1" ]]; then
    continue
  fi

  if ! gzip -t "$input_path"; then
    echo "Invalid gzip, skipping: ${input_path}" >&2
    FAILED+=("$input_name")
    continue
  fi

  tmp_output_path="${output_path}.tmp"
  rm -f "$tmp_output_path"

  "$PYTHON" "${SCRIPT_DIR}/prepare_delay_baseline_dataset.py" \
    --input-glob "$input_path" \
    --output "$tmp_output_path" \
    --max-files 1 \
    "$@" || {
      echo "Failed to build: ${input_path}" >&2
      rm -f "$tmp_output_path"
      FAILED+=("$input_name")
      continue
    }

  mv "$tmp_output_path" "$output_path"
done

echo
if [[ "${#FAILED[@]}" -gt 0 ]]; then
  echo "Done with ${#FAILED[@]} failed file(s):" >&2
  printf ' - %s\n' "${FAILED[@]}" >&2
  exit 1
fi

echo "Done."
