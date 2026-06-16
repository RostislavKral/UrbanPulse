#!/usr/bin/env bash

set -euo pipefail

VPS_USER="${VPS_USER:-}"
VPS_HOST="${VPS_HOST:-}"
REMOTE="${VPS_USER}@${VPS_HOST}"
MAX_LOG_MB="${MAX_LOG_MB:-100}"

if [[ -z "$VPS_USER" || -z "$VPS_HOST" ]]; then
  echo "VPS_USER and VPS_HOST must be set." >&2
  exit 1
fi

ssh "$REMOTE" "MAX_LOG_MB='$MAX_LOG_MB' bash -s" <<'REMOTE_SCRIPT'
set -euo pipefail

mkdir -p /etc/docker
if [ -f /etc/docker/daemon.json ]; then
  cp -a /etc/docker/daemon.json "/etc/docker/daemon.json.bak.$(date -u +%Y%m%dT%H%M%SZ)"
fi

cat > /etc/docker/daemon.json <<'JSON'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "20m",
    "max-file": "5"
  }
}
JSON

echo "Wrote /etc/docker/daemon.json. Restart Docker later for this to affect new log files."

for container in $(docker ps -q); do
  name=$(docker inspect -f "{{.Name}}" "$container" | sed "s#^/##")
  log_path=$(docker inspect -f "{{.LogPath}}" "$container")
  [ -f "$log_path" ] || continue

  size_mb=$(du -m "$log_path" | cut -f1)
  if [ "$size_mb" -gt "$MAX_LOG_MB" ]; then
    echo "Truncating ${name} log: ${size_mb}M -> 0"
    : > "$log_path"
  else
    echo "Keeping ${name} log: ${size_mb}M"
  fi
done

df -h /
REMOTE_SCRIPT
