# GTFS Static Data

This folder stores static GTFS files used for dataset preparation.

## Source
- Provider: PID (Prague Integrated Transport)
- URL: `https://data.pid.cz/PID_GTFS.zip`

## Current snapshot
- Downloaded: 2026-02-07
- Extracted to: `db/gtfs/pid_static/`

Key files for joins:
- `stops.txt`
- `routes.txt`
- `trips.txt`
- `stop_times.txt`

## Refresh
```bash
mkdir -p db/gtfs/pid_static
cd db/gtfs/pid_static
curl -fL --retry 3 -o PID_GTFS.zip https://data.pid.cz/PID_GTFS.zip
unzip -o PID_GTFS.zip
```
