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
echo "== system =="
hostname
date -Is
uptime

echo
echo "== disk =="
df -h /
du -h -d1 /opt /var/lib/docker 2>/dev/null | sort -h | tail -30 || true

echo
echo "== memory =="
free -h

echo
echo "== docker compose =="
docker compose ls || true
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
docker ps -s --format "table {{.Names}}\t{{.Size}}"
docker system df || true

echo
echo "== docker logs =="
for container in $(docker ps -q); do
  name=$(docker inspect -f "{{.Name}}" "$container" | sed "s#^/##")
  log_path=$(docker inspect -f "{{.LogPath}}" "$container")
  log_size=$(du -h "$log_path" 2>/dev/null | cut -f1 || true)
  echo "$name ${log_size:-unknown} $log_path"
done

echo
echo "== urban pulse =="
if [ -d /opt/urban-pulse ]; then
  du -h -d2 /opt/urban-pulse 2>/dev/null | sort -h | tail -40
  if [ -d /opt/urban-pulse/.git ]; then
    git -C /opt/urban-pulse status --short || true
    git -C /opt/urban-pulse log -1 --oneline || true
  else
    echo "/opt/urban-pulse is not a git checkout"
  fi
fi

echo
echo "== postgres =="
if docker ps --format "{{.Names}}" | grep -qx prague_db; then
  docker exec prague_db sh -lc '"'"'
    echo "PGDATA=$PGDATA"
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "show data_directory;"
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select pg_size_pretty(pg_database_size(current_database())) as db_size;"
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "select min(time), max(time) from vehicle_positions;"
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select job_id, application_name, schedule_interval, config from timescaledb_information.jobs order by job_id;"
  '"'"'
fi
'
