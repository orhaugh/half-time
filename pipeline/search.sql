-- The menu search: every change to a menu, indexed.
--
-- Each change is one document, named by the restaurant, the item and the
-- time of the change, so a change indexed twice is the same document and
-- the index holds each menu as it was at any point of the night. At least
-- once: a restart can index a document again, and the name makes that an
-- overwrite.

CREATE TABLE menu_changes (
    restaurant_id VARCHAR,
    item_id       VARCHAR,
    name          VARCHAR,
    price_cents   BIGINT,
    available     BIGINT,
    updated_at    BIGINT,
    op            VARCHAR
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'menus.changes',
    group_id          = 'search',
    auto_offset_reset = 'earliest'
);

CREATE TABLE menu_index (
    doc_id        VARCHAR,
    restaurant_id VARCHAR,
    item_id       VARCHAR,
    name          VARCHAR,
    price_cents   BIGINT,
    available     BIGINT,
    updated_at    BIGINT,
    op            VARCHAR
) WITH (
    connector          = 'opensearch',
    format             = 'json',
    url                = 'http://opensearch:9200',
    index              = 'menus',
    document_id        = 'doc_id',
    delivery_guarantee = 'at_least_once'
);

INSERT INTO menu_index
SELECT restaurant_id || ':' || item_id || ':' || CAST(updated_at AS VARCHAR) AS doc_id,
       restaurant_id, item_id, name, price_cents, available, updated_at, op
FROM menu_changes;
