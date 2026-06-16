#!/usr/bin/env bash

set -euo pipefail

VPS_USER="${VPS_USER:-}"
VPS_HOST="${VPS_HOST:-}"
REMOTE="${VPS_USER}@${VPS_HOST}"

if [[ -z "$VPS_USER" || -z "$VPS_HOST" ]]; then
  echo "VPS_USER and VPS_HOST must be set." >&2
  exit 1
fi

ssh "$REMOTE" 'set -euo pipefail
backup_dir="${BACKUP_DIR:-/opt/urban-pulse/backups/schema}"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_dir"

docker exec prague_db sh -lc '"'"'
  pg_dumpall -U "$POSTGRES_USER" --globals-only
'"'"' | gzip > "${backup_dir}/globals_${stamp}.sql.gz"

docker exec prague_db sh -lc '"'"'
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --schema-only --no-owner --no-privileges
'"'"' | gzip > "${backup_dir}/schema_${stamp}.sql.gz"

find "$backup_dir" -type f -name "*.gz" -mtime +14 -delete
ls -lh "$backup_dir" | tail -20
'
