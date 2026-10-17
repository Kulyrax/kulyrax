-- The producer's OWN state — not Kulyrax's projection tables, and not even
-- the same Postgres *database* as Kulyrax's projection destination in the
-- Docker Compose setup (one Postgres instance, two databases: `kulyrax` and
-- `producer_ledger`), so a glance at either one's tables can never be
-- confused with the other's.

CREATE TABLE IF NOT EXISTS processed_files (
    file_name     TEXT PRIMARY KEY,
    content_hash  TEXT NOT NULL,
    -- 'in_progress' | 'succeeded' | 'failed'
    status        TEXT NOT NULL,
    failure_reason TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

-- The last committed LIVE inventory snapshot — a row exists here iff that
-- vehicle was present in the most recently *fully succeeded* run. Deleted on
-- VehicleSold, inserted fresh on a later reappearance, so the comparison
-- `previous` dictionary is just "SELECT * FROM vehicle_baseline".
CREATE TABLE IF NOT EXISTS vehicle_baseline (
    provider_vehicle_id TEXT PRIMARY KEY,
    vin            TEXT NOT NULL,
    make           TEXT NOT NULL,
    model          TEXT NOT NULL,
    year           INT NOT NULL,
    price          NUMERIC NOT NULL,
    mileage        INT NOT NULL,
    condition      TEXT NOT NULL,
    location       TEXT NOT NULL,
    listing_status TEXT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The durable, monotonically increasing per-vehicle sequence Kulyrax's own
-- guard semantics require (producer-contract.md §1). Deliberately a
-- SEPARATE table from vehicle_baseline and NEVER deleted: a vehicle that is
-- sold and later reappears still must not reuse a sequence number Kulyrax
-- has already seen for that entity, even though its baseline row was
-- deleted when it sold.
CREATE TABLE IF NOT EXISTS vehicle_sequence (
    provider_vehicle_id TEXT PRIMARY KEY,
    last_sequence BIGINT NOT NULL DEFAULT 0
);

-- One row per generated event. Persisted (step 5 of the task's required
-- behavior) before the first send attempt; a retry or a resumed run after a
-- restart re-sends exactly this row's id/payload, never a freshly built one.
CREATE TABLE IF NOT EXISTS producer_events (
    event_id            TEXT PRIMARY KEY,
    file_name           TEXT NOT NULL REFERENCES processed_files(file_name),
    provider_vehicle_id TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    sequence            BIGINT NOT NULL,
    position            TEXT NOT NULL,
    payload             TEXT NOT NULL,
    -- 'pending' | 'acked' | 'failed'
    status              TEXT NOT NULL DEFAULT 'pending',
    failure_reason       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_producer_events_file ON producer_events (file_name, position);
