-- The legacy menu system: every restaurant's menu, as the chain's old
-- point-of-sale keeps it, read by clink from the binary log. The capture
-- user needs the replication privileges and RELOAD for the snapshot lock.
CREATE TABLE IF NOT EXISTS menus (
    restaurant_id VARCHAR(32) NOT NULL,
    item_id       VARCHAR(32) NOT NULL,
    name          VARCHAR(64) NOT NULL,
    price_cents   BIGINT      NOT NULL,
    available     TINYINT(1)  NOT NULL DEFAULT 1,
    updated_at    BIGINT      NOT NULL,
    PRIMARY KEY (restaurant_id, item_id)
);
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
