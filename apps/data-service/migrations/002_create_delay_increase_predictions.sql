CREATE TABLE IF NOT EXISTS delay_increase_predictions (
    scored_at                         TIMESTAMPTZ       NOT NULL,
    vehicle_id                        TEXT              NOT NULL,
    position_time                     TIMESTAMPTZ       NOT NULL,
    delay_increase_risk               DOUBLE PRECISION  NOT NULL,
    raw_delay_increase_alert          BOOLEAN           NOT NULL DEFAULT FALSE,
    delay_increase_alert              BOOLEAN           NOT NULL,
    alert_policy_reason               TEXT,
    alert_rank                        INTEGER,
    threshold                         DOUBLE PRECISION  NOT NULL,
    alert_min_risk                    DOUBLE PRECISION  NOT NULL DEFAULT 0,
    model_path                        TEXT,
    model_mtime                       DOUBLE PRECISION,
    score_source                      TEXT              NOT NULL DEFAULT 'realtime',
    context_minutes                   INTEGER,
    sampling_seconds                  INTEGER,
    output_freshness_seconds          INTEGER,
    line                              TEXT,
    route_id                          TEXT,
    trip_id                           TEXT,
    mode                              TEXT,
    state_position                    TEXT,
    delay                             INTEGER,
    actual_increase_60s               INTEGER,
    delay_delta_1                     DOUBLE PRECISION,
    delay_mean_3                      DOUBLE PRECISION,
    speed                             DOUBLE PRECISION,
    speed_delta_1                     DOUBLE PRECISION,
    last_stop_id                      TEXT,
    next_stop_id                      TEXT,
    last_stop_sequence                INTEGER,
    next_stop_sequence                INTEGER,
    lat                               DOUBLE PRECISION,
    lon                               DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_delay_predictions_scored_at
    ON delay_increase_predictions (scored_at DESC);

CREATE INDEX IF NOT EXISTS idx_delay_predictions_vehicle_scored
    ON delay_increase_predictions (vehicle_id, scored_at DESC);

CREATE INDEX IF NOT EXISTS idx_delay_predictions_alerts
    ON delay_increase_predictions (scored_at DESC, delay_increase_risk DESC)
    WHERE delay_increase_alert;

CREATE INDEX IF NOT EXISTS idx_delay_predictions_route_time
    ON delay_increase_predictions (route_id, position_time DESC);
