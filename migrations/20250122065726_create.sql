-- Add migration script here
CREATE TABLE runepool_unit_intervals (
id BIGSERIAL PRIMARY KEY,
start_time TIMESTAMPTZ NOT NULL,
end_time TIMESTAMPTZ NOT NULL,
count BIGINT NOT NULL CHECK (count >= 0),
units BIGINT NOT NULL CHECK (units >= 0),
created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_runepool_units_time_range ON runepool_unit_intervals (start_time, end_time);