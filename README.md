# UrbanPulse

UrbanPulse is a small, work-in-progress project for collecting and exploring Prague public transport data. It started as a real-time map, but gradually grew into a data pipeline for replay, basic traffic analysis, and early delay-prediction experiments.

The system collects vehicle positions from Golemio, stores them in PostgreSQL/TimescaleDB, publishes live updates through Redis/WebSocket, and renders vehicles on a React map. It also includes replay tooling so past traffic can be inspected instead of only watching the live feed.

So far, the focus has been on exporting real collected data, preparing delay-prediction datasets, and comparing simple baselines before trying more complex graph-based models.

## Current Focus

- real-time public transport visualization
- replay of historical vehicle movement
- storage and retention for larger telemetry data
- baseline delay prediction experiments
- groundwork for future graph-based modelling

## Tech Stack

- Python, FastAPI, asyncpg
- Redis and WebSocket streaming
- PostgreSQL / TimescaleDB
- React, Deck.gl, MapLibre
- Polars and scikit-learn for early ML experiments

## CI, Tests, and Deployment

GitHub Actions run CI on pull requests and pushes to `main`/`master`. The pipeline
installs dependencies, runs linting and tests for the frontend, realtime gateway,
and Python services, then validates and builds the Docker images.

Useful local checks:

```bash
cd apps/frontend && npm run lint && npm test && npm run build
cd apps/realtime-gateway && npm run lint && npm test && npm run build
python -m pip install -r requirements-dev.txt
python -m ruff check apps/data-service ml/scripts tests/python
python -m pytest
```

CI also runs advisory vulnerability scans: `npm audit`, `pip-audit`, and Trivy
filesystem/container scans for high and critical findings. These steps are
currently non-blocking so existing findings can be triaged without freezing all
PRs; remove `continue-on-error: true` from a scan step when you want it to become
a release gate.

Docker images are published to GitHub Container Registry by the publish workflow.
Kubernetes manifests live in `deploy/kubernetes`; see
`deploy/kubernetes/README.md` for secret setup, image configuration, and deploy
commands.

Still under construction. It is a playground for learning how real-time transport data behaves, where the data quality problems are, and what needs to be true before more advanced ML approaches are worth using.
