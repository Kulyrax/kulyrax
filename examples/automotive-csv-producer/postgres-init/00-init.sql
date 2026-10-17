-- Runs once, automatically, the first time the `postgres` container starts
-- with an empty data directory (standard docker-entrypoint-initdb.d
-- behavior). One Postgres instance, two databases, so a glance at either
-- one's tables can never be confused with the other's:
--   kulyrax         — Kulyrax's own projection destination (the `vehicles` table)
--   producer_ledger — the C# producer's own state (see Ledger/LedgerSchema.sql,
--                     applied by the app itself on startup, not here)
CREATE DATABASE kulyrax;
CREATE DATABASE producer_ledger;

\connect kulyrax

-- `price` is DOUBLE PRECISION, not NUMERIC: kulyrax-adapter-postgres's
-- column-type introspection (PgColType::from_data_type) only recognizes a
-- fixed set of Postgres type names and falls back to a text bind for
-- anything else, including `numeric` — which rust-postgres's extended
-- query protocol then rejects at parameter-encoding time ("error
-- serializing parameter N"), since a text-typed bind isn't accepted for a
-- server-negotiated NUMERIC parameter slot. This is a real, pre-existing
-- gap in the adapter (proper NUMERIC support needs a decimal type with its
-- own ToSql impl, a bigger change than this example warrants); using
-- `double precision` for price here is the smallest correct way around it
-- for this demo, not a fix to the gap itself. The C# producer still
-- validates and carries price as a precise `decimal` end to end — only
-- this one destination column's storage type is affected.
CREATE TABLE IF NOT EXISTS vehicles (
    provider_vehicle_id TEXT PRIMARY KEY,
    vin                 TEXT NOT NULL,
    make                TEXT NOT NULL,
    model                TEXT NOT NULL,
    year                 INT NOT NULL,
    price                DOUBLE PRECISION NOT NULL,
    mileage              INT NOT NULL,
    condition            TEXT NOT NULL,
    location             TEXT NOT NULL,
    listing_status       TEXT NOT NULL,
    sold                 BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
