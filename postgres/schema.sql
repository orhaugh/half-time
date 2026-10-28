-- The order store: the platform's system of record for orders, which clink
-- reads as a change stream, and the ledgers clink writes exactly once.
--
-- Times are epoch milliseconds, the form clink carries them in; the readable
-- timestamps are derived here.

-- One row per order, updated as it moves through its life: placed, accepted,
-- picked_up, then delivered or cancelled. REPLICA IDENTITY FULL puts the whole
-- row into every change, which is what the change stream carries.
CREATE TABLE IF NOT EXISTS orders (
    order_id      text   PRIMARY KEY,
    cell          text   NOT NULL,   -- the city
    zone          text   NOT NULL,   -- the delivery zone
    restaurant_id text   NOT NULL,
    customer_id   text   NOT NULL,
    cx            bigint NOT NULL,   -- the delivery point, metres from the city's origin
    cy            bigint NOT NULL,
    placed_at     bigint NOT NULL,
    promised_at   bigint NOT NULL,
    fee_cents     bigint NOT NULL,   -- the delivery fee the customer paid
    total_cents   bigint NOT NULL,   -- the basket
    status        text   NOT NULL,
    updated_at    bigint NOT NULL,
    placed        timestamptz GENERATED ALWAYS AS (to_timestamp(placed_at / 1000.0)) STORED,
    updated       timestamptz GENERATED ALWAYS AS (to_timestamp(updated_at / 1000.0)) STORED
);
ALTER TABLE orders REPLICA IDENTITY FULL;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'halftime_orders') THEN
    CREATE PUBLICATION halftime_orders FOR TABLE orders WITH (publish = 'insert, update, delete');
  END IF;
END $$;
-- The one user: it owns the tables and streams the publication.
ALTER ROLE halftime REPLICATION;

-- A rider's pay for one delivery, written exactly once by pipeline/ledgers.sql.
-- order_id carries no uniqueness constraint on purpose: a second copy would
-- land as a second row, where the run's check counts it, rather than fail the
-- ledger's transaction over and over.
CREATE TABLE IF NOT EXISTS payouts (
    order_id        text   NOT NULL,
    rider_id        bigint NOT NULL,
    cell            text   NOT NULL,
    zone            text   NOT NULL,
    fee_cents       bigint NOT NULL,
    multiplier_pct  bigint NOT NULL,
    amount_cents    bigint NOT NULL,
    delivered_at_ms bigint NOT NULL,
    delivered_at    timestamptz GENERATED ALWAYS AS (to_timestamp(delivered_at_ms / 1000.0)) STORED,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS payouts_order_id ON payouts (order_id);

-- A credit to a customer, written exactly once by pipeline/ledgers.sql:
-- late_delivery (a fifth of the basket) or cancelled (the basket).
CREATE TABLE IF NOT EXISTS credits (
    order_id     text   NOT NULL,
    customer_id  text   NOT NULL,
    cell         text   NOT NULL,
    reason       text   NOT NULL,
    amount_cents bigint NOT NULL,
    at_ms        bigint NOT NULL,
    at           timestamptz GENERATED ALWAYS AS (to_timestamp(at_ms / 1000.0)) STORED,
    recorded_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS credits_order_id ON credits (order_id);

-- Each zone's current surge multiplier, kept up to the latest pricing window
-- by pipeline/pricing.sql: one row per zone, replaced as each window closes.
CREATE TABLE IF NOT EXISTS zone_pricing (
    zone           text   PRIMARY KEY,
    cell           text   NOT NULL,
    multiplier_pct bigint NOT NULL,
    demand         bigint NOT NULL,
    supply         bigint NOT NULL,
    window_end_ms  bigint NOT NULL,
    window_end     timestamptz GENERATED ALWAYS AS (to_timestamp(window_end_ms / 1000.0)) STORED
);
