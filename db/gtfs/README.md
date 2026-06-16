# GTFS Static Data

This folder stores static GTFS files used during dataset preparation. The files
describe stops, routes, trips, and scheduled stop times for Prague Integrated
Transport, and they are useful for joining live vehicle observations with route
and stop topology.

## Current Snapshot

- Provider: PID, Prague Integrated Transport
- Source: `https://data.pid.cz/PID_GTFS.zip`
- Downloaded: 2026-02-07
- Extracted to: `db/gtfs/pid_static/`

The most important join files are:

- `stops.txt`
- `routes.txt`
- `trips.txt`
- `stop_times.txt`

## How It Is Refreshed

The local snapshot has been refreshed from the public PID archive with:

```bash
mkdir -p db/gtfs/pid_static
cd db/gtfs/pid_static
curl -fL --retry 3 -o PID_GTFS.zip https://data.pid.cz/PID_GTFS.zip
unzip -o PID_GTFS.zip
```

## What Comes Next

GTFS data is expected to become more important once graph-based modelling is
started. The static route and stop topology can then be combined with recent
vehicle state to build graph snapshots for GNN experiments.
