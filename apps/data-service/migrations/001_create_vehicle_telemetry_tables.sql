CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS vehicle_positions (
    time                      TIMESTAMPTZ       NOT NULL,
    vehicle_id                TEXT              NOT NULL,
    lat                       DOUBLE PRECISION  NOT NULL,
    lon                       DOUBLE PRECISION  NOT NULL,
    line                      TEXT,
    delay                     INTEGER,
    speed                     INTEGER,
    route_id                  TEXT,
    mode                      TEXT,
    route_type                INTEGER,
    trip_id                   TEXT,
    state_position            TEXT,
    origin_timestamp          TIMESTAMPTZ,
    last_stop_id              TEXT,
    last_stop_sequence        INTEGER,
    last_stop_arrival_time    TIMESTAMPTZ,
    last_stop_departure_time  TIMESTAMPTZ,
    next_stop_id              TEXT,
    next_stop_sequence        INTEGER,
    next_stop_arrival_time    TIMESTAMPTZ,
    next_stop_departure_time  TIMESTAMPTZ
);

ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS route_id TEXT;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS trip_id TEXT;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS mode TEXT;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS route_type INTEGER;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS state_position TEXT;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS origin_timestamp TIMESTAMPTZ;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS last_stop_id TEXT;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS last_stop_sequence INTEGER;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS last_stop_arrival_time TIMESTAMPTZ;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS last_stop_departure_time TIMESTAMPTZ;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS next_stop_id TEXT;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS next_stop_sequence INTEGER;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS next_stop_arrival_time TIMESTAMPTZ;
ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS next_stop_departure_time TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS vehicle_trajectory_points (
    time                  TIMESTAMPTZ       NOT NULL,
    vehicle_id            TEXT              NOT NULL,
    lat                   DOUBLE PRECISION  NOT NULL,
    lon                   DOUBLE PRECISION  NOT NULL,
    point_state           TEXT              NOT NULL,
    confidence            TEXT              NOT NULL,
    interpolation_method  TEXT,
    gap_reason            TEXT,
    route_id              TEXT,
    trip_id               TEXT,
    mode                  TEXT
);

ALTER TABLE vehicle_trajectory_points ADD COLUMN IF NOT EXISTS route_id TEXT;
ALTER TABLE vehicle_trajectory_points ADD COLUMN IF NOT EXISTS trip_id TEXT;
ALTER TABLE vehicle_trajectory_points ADD COLUMN IF NOT EXISTS mode TEXT;

CREATE INDEX IF NOT EXISTS idx_vehicle_id
    ON vehicle_positions (vehicle_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_traj_vehicle_time
    ON vehicle_trajectory_points (vehicle_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_traj_time_vehicle
    ON vehicle_trajectory_points (time ASC, vehicle_id ASC);
