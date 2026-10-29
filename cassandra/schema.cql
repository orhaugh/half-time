-- The customer app's order history: one row per change to an order, keyed
-- by the order and the time of the change, so a change written twice is the
-- same row. The sink does not create schema, so this runs before the jobs.
CREATE KEYSPACE IF NOT EXISTS halftime WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS halftime.order_history (
    order_id    text,
    updated_at  bigint,
    cell        text,
    status      text,
    PRIMARY KEY (order_id, updated_at)
);
