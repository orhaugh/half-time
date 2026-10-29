-- The match feed: what happened on the pitch, polled from the broadcaster,
-- announced on Kafka, kept in the analytics store, and joined to the orders
-- each goal set off.
--
-- The feed is an HTTP endpoint polled every second with a cursor. The job
-- announces each event on a topic, keeps the timeline in ClickHouse, and
-- joins the order stream to the goals: an order placed in the two minutes
-- from a goal is that goal's. The feed is polled once; the timeline and the
-- join read the topic the job itself writes. The polling source keeps no
-- position clink can restore (a restart polls from the first cursor again,
-- and the feed keeps everything it has released), so the job is at least
-- once throughout: the events are made distinct before the join, the
-- timeline collapses its copies by event, and a goal's orders by goal and
-- order.

CREATE TABLE match_feed (
    seq    BIGINT,
    minute BIGINT,
    kind   VARCHAR,
    detail VARCHAR,
    ts     BIGINT
) WITH (
    connector        = 'http_poll',
    format           = 'json',
    url              = 'http://match-feed:28090',
    path             = '/events',
    records_field    = 'items',
    cursor_param     = 'since',
    cursor_field     = 'seq',
    initial_cursor   = '0',
    poll_interval_ms = '1000'
);

CREATE TABLE match_events (
    seq    BIGINT,
    minute BIGINT,
    kind   VARCHAR,
    detail VARCHAR,
    ts     BIGINT
) WITH (
    connector = 'kafka',
    format    = 'json',
    brokers   = 'kafka:9092',
    topic     = 'match.events'
);

CREATE TABLE match_timeline (
    seq    BIGINT,
    minute BIGINT,
    kind   VARCHAR,
    detail VARCHAR,
    ts_ms  BIGINT
) WITH (
    connector         = 'clickhouse',
    insert_format     = 'native',
    host              = 'clickhouse',
    port              = '9000',
    database          = 'halftime',
    "table"           = 'match_timeline',
    user              = 'halftime',
    password          = 'halftime',
    batch_interval_ms = '500'
);

CREATE TABLE order_changes (
    order_id      VARCHAR,
    cell          VARCHAR,
    zone          VARCHAR,
    restaurant_id VARCHAR,
    customer_id   VARCHAR,
    cx            BIGINT,
    cy            BIGINT,
    placed_at     BIGINT,
    promised_at   BIGINT,
    fee_cents     BIGINT,
    total_cents   BIGINT,
    status        VARCHAR,
    updated_at    BIGINT,
    op            VARCHAR,
    version       BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'orders.changes',
    group_id          = 'match',
    auto_offset_reset = 'earliest',
    event_time_column = 'updated_at',
    watermark_lag_ms  = '60000',
    idle_timeout_ms   = '90000'
);

CREATE TABLE events (
    seq    BIGINT,
    minute BIGINT,
    kind   VARCHAR,
    detail VARCHAR,
    ts     BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'match.events',
    group_id          = 'match',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'ts',
    watermark_lag_ms  = '60000',
    idle_timeout_ms   = '90000'
);

CREATE TABLE goal_orders (
    goal_seq     BIGINT,
    goal_minute  BIGINT,
    order_id     VARCHAR,
    cell         VARCHAR,
    placed_at_ms BIGINT
) WITH (
    connector         = 'clickhouse',
    insert_format     = 'native',
    host              = 'clickhouse',
    port              = '9000',
    database          = 'halftime',
    "table"           = 'goal_orders',
    user              = 'halftime',
    password          = 'halftime',
    batch_interval_ms = '500'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO match_events SELECT seq, minute, kind, detail, ts FROM match_feed;

INSERT INTO match_timeline SELECT seq, minute, kind, detail, ts AS ts_ms FROM events;

INSERT INTO goal_orders
WITH placed AS (
    SELECT order_id, cell, updated_at, CAST(1 AS BIGINT) AS k FROM order_changes WHERE op = 'insert'
),
goal AS (
    SELECT DISTINCT seq, minute, ts, CAST(1 AS BIGINT) AS k FROM events WHERE kind = 'goal'
)
SELECT g.seq AS goal_seq, g.minute AS goal_minute, p.order_id, p.cell, p.updated_at AS placed_at_ms
FROM placed p JOIN goal g
  ON p.k = g.k AND p.updated_at BETWEEN g.ts + 0 AND g.ts + 120000;
END;
