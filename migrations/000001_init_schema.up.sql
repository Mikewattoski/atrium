CREATE TABLE devices (
    id          TEXT PRIMARY KEY,
    name        TEXT        NOT NULL,
    type        TEXT        NOT NULL,
    location    TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE readings (
    id         BIGSERIAL PRIMARY KEY,
    device_id  TEXT             NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
    metric     TEXT             NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    ts         TIMESTAMPTZ      NOT NULL,
    CONSTRAINT readings_unique UNIQUE (device_id, metric, ts)
);

CREATE INDEX idx_readings_device_ts ON readings (device_id, ts DESC);

CREATE TABLE alerts (
    id           BIGSERIAL PRIMARY KEY,
    device_id    TEXT             NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
    metric       TEXT             NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    threshold    DOUBLE PRECISION NOT NULL,
    status       TEXT             NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    created_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_alerts_one_open
    ON alerts (device_id, metric)
    WHERE status = 'open';

CREATE INDEX idx_alerts_created ON alerts (created_at DESC);
