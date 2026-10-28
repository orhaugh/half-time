-- The analytics store. clink writes these tables through the native protocol,
-- at least once: after a recovery some rows are here twice, identically, and
-- each table's key collapses them (SELECT ... FINAL gives the current state
-- whatever has merged). Times arrive as epoch milliseconds; the readable
-- columns are materialised here.

-- Every change to an order, as the order store committed it.
CREATE TABLE IF NOT EXISTS halftime.order_events
(
    order_id      String,
    cell          LowCardinality(String),
    zone          LowCardinality(String),
    restaurant_id LowCardinality(String),
    customer_id   String,
    status        LowCardinality(String),
    fee_cents     Int64,
    total_cents   Int64,
    placed_at_ms  Int64,
    promised_at_ms Int64,
    updated_at_ms Int64,
    op            LowCardinality(String),
    version       UInt64,
    placed_at     DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(placed_at_ms),
    updated_at    DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(updated_at_ms)
)
ENGINE = ReplacingMergeTree
ORDER BY (order_id, version);

-- Each order assigned to a rider, or escalated because no rider was near, in
-- which case it has no rider.
CREATE TABLE IF NOT EXISTS halftime.dispatch_decisions
(
    order_id       String,
    kind           LowCardinality(String),
    rider_id       Nullable(Int64),
    candidates     Int64,
    score          Int64,
    multiplier_pct Int64,
    fee_cents      Int64,
    cell           LowCardinality(String),
    zone           LowCardinality(String),
    restaurant_id  LowCardinality(String),
    placed_at_ms   Int64,
    placed_at      DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(placed_at_ms)
)
ENGINE = ReplacingMergeTree
ORDER BY order_id;

-- Each zone's pricing window: demand, supply and the multiplier they gave.
CREATE TABLE IF NOT EXISTS halftime.zone_pricing_windows
(
    cell           LowCardinality(String),
    zone           LowCardinality(String),
    window_end_ms  Int64,
    demand         Int64,
    supply         Int64,
    multiplier_pct Int64,
    window_end     DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(window_end_ms)
)
ENGINE = ReplacingMergeTree
ORDER BY (zone, window_end_ms);

-- Each payout the ledger recorded, as the ledger published it.
CREATE TABLE IF NOT EXISTS halftime.rider_payouts
(
    order_id        String,
    rider_id        Int64,
    cell            LowCardinality(String),
    zone            LowCardinality(String),
    fee_cents       Int64,
    multiplier_pct  Int64,
    amount_cents    Int64,
    delivered_at_ms Int64,
    delivered_at    DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(delivered_at_ms)
)
ENGINE = ReplacingMergeTree
ORDER BY order_id;

-- Each rider's pay, delivery by delivery, with the running total. A payout
-- reaches the ledger when the later of the delivery and the dispatch decision
-- arrives, so the payouts do not arrive in delivery order; ClickHouse puts
-- them in order here, over the collapsed rows.
CREATE VIEW IF NOT EXISTS halftime.rider_earnings AS
SELECT rider_id, cell, order_id, amount_cents,
       sum(amount_cents) OVER (PARTITION BY rider_id ORDER BY delivered_at_ms, order_id) AS running_cents,
       delivered_at_ms
FROM (SELECT * FROM halftime.rider_payouts FINAL);

-- What each customer has spent tonight, delivery by delivery: clink's running
-- total over the order stream, in event time.
CREATE TABLE IF NOT EXISTS halftime.customer_spend
(
    customer_id     String,
    cell            LowCardinality(String),
    order_id        String,
    total_cents     Int64,
    spent_cents     Int64,
    delivered_at_ms Int64,
    delivered_at    DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(delivered_at_ms)
)
ENGINE = ReplacingMergeTree
ORDER BY (customer_id, order_id);
