# UrbanPulse

UrbanPulse is a small, work-in-progress project for collecting and exploring Prague public transport data. It started as a real-time map, but gradually grew into a data pipeline for replay, basic traffic analysis, and early delay-prediction experiments.

The system collects vehicle positions from Golemio, stores them in PostgreSQL/TimescaleDB, publishes live updates through Redis/WebSocket, and renders vehicles on a React map. It also includes replay tooling so past traffic can be inspected instead of only watching the live feed.

The ML part is still early and intentionally modest. So far, the focus has been on exporting real collected data, preparing delay-prediction datasets, and comparing simple baselines before trying more complex graph-based models.

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

This is not a finished product. It is an engineering playground for learning how real-time transport data behaves, where the data quality problems are, and what needs to be true before more advanced ML approaches are worth using.
