# half-time

A delivery platform on the night of a world final, run as a stream on
[clink](https://github.com/orhaugh/clink). Three cities order food through
ninety minutes, extra time and penalties, and a billion people order at once
at half-time. Riders report where they are every twelve seconds, and stop
reporting when they stop to watch a goal. Every order moves through its life
in a PostgreSQL order store, and everything the platform does with it happens
in clink: the order stream is announced exactly once, each zone is priced
every minute from the last five minutes of demand and supply, each order is
quoted under the price it was placed at, assigned to the nearest rider or
escalated, and when it is delivered the rider is paid and a late customer is
credited, exactly once, in PostgreSQL. Everything is also counted in
ClickHouse and archived as Parquet. Around the core, the restaurants' menus
live in a MySQL system that will stay MySQL, their tablets talk on Pulsar, a
kitchen that bounces three orders in a row is reported to the floor over
NATS, an order no rider was near goes to the operations desk's RabbitMQ queue
and to the partners' Pulsar topic, and every order's latest state is kept in
Redis and its history in Cassandra. The riders' phones report their
batteries over MQTT, and a job built on clink's C++ API keeps each rider's
latest status in the rider app's MongoDB store and tells a rider whose
battery runs low to charge. The broadcaster's feed is polled over
HTTP and each goal's surge of orders is counted, every menu change is
indexed in OpenSearch, every rider position is a point in InfluxDB, the order
stream is a Delta table, each zone's multiplier is a gauge Prometheus scrapes
and Grafana shows beside clink's own metrics, and after the night a bounded
job reads the dispatch archive back.

It is a deliberately convoluted pipeline, because real ones are. Nineteen
jobs, eighteen in SQL and one built on clink's C++ API, read and write ten
Kafka topics, a PostgreSQL change stream, a MySQL change stream, an HTTP
feed, two PostgreSQL ledgers and a price table, a Neo4j city graph, eight
ClickHouse tables, three Parquet archives, a Delta table, two Pulsar topics,
a RabbitMQ queue, a NATS stream, two Redis keyspaces, a Cassandra table, an
OpenSearch index, an InfluxDB bucket, a Pushgateway, an MQTT broker's device
and command topics and a MongoDB collection, and the outputs of one job are
the inputs of the next. A worker is killed in the middle of the half-time
surge, producers resend batches they had already delivered, and every store
is then checked, row by row, against a reference model that never touched
clink.

## What it shows

- **One order store, read once.** The order service writes PostgreSQL and
  nothing else. clink reads the table's replication stream and announces each
  change exactly once on Kafka; every other job reads that topic.
- **Outputs that are inputs.** Pricing windows price the quotes, quotes feed
  the dispatcher, dispatch decisions and delivery events meet in the ledger,
  and the ledger's payouts feed the analytics store. Each hop is a Kafka topic
  written in a transaction committed with the job's checkpoint, so a job reads
  only what the job before it has committed.
- **Event time, under a kill.** Prices, quotes and dispatch decisions are
  windows in event time, so the results depend on when things happened, not
  on when they arrived. The worker that is killed mid-surge comes back from
  its last checkpoint, and the reference model does not notice.
- **One guarantee per output, each declared.** The order stream, the quotes,
  the decisions, the payouts and the credits are written exactly once. The
  price table is kept by upsert. The analytics store is written at least
  once and collapses the copies. The archive is published exactly once, by
  rename on checkpoint.
- **A graph in the hot path.** Where a restaurant is and which zones touch its
  own come from Neo4j, looked up per order, inside the dispatch query.
- **Two change streams, two databases.** The order store is PostgreSQL,
  read through a replication slot; the menu system is MySQL, read from its
  binary log, the existing rows first as a snapshot at a consistent position.
  Each is announced on Kafka exactly once, and neither system knows.
- **A pattern over a bus that repeats itself.** The tablets' events arrive
  on Pulsar, which delivers at least once. The kitchen job makes them
  distinct before `MATCH_RECOGNIZE` looks for three bounces in a row per
  restaurant, in event time, so a repeated event, from a restart or a retry,
  cannot count as a bounce of its own. The alert itself goes out at least
  once: clink refuses an exactly-once sink in a job over a Pulsar source, so
  the alerts topic is declared at least once. The bus is read through an
  exclusive subscription, so the source records its position at each
  checkpoint, a restart seeks back to it and replays the bus in order, and
  the watch allows the same minute of lateness as the Kafka tables. Through
  the default shared subscription the broker hands a restarted consumer its
  unacknowledged messages after the newer ones, and the watch needed
  forty-five minutes of match time, the ninety seconds a restart and a
  redelivery take at thirty times real time, to take them back in.
- **Stores kept by key.** The tracker ranks each order's changes by version
  and keeps the top one in Redis, so a change that arrives behind a later one
  cannot win, and the key is replaced or restored as the ranking changes.
  Cassandra holds every change as a row keyed by order and time. A restart
  that writes a state again changes nothing in either.
- **Sinks that declare what they are.** clink's planner computes each job's
  guarantee from its sources and sinks and refuses a declaration the path
  cannot honour. A sink over the Pulsar bus cannot claim exactly once,
  because the bus redelivers rather than replays, and the SQL says so. The
  polled match feed checkpoints its cursor and the feed honours it, so its
  events reach Kafka exactly once through a transactional sink, and the
  planner accepts that declaration because the connector's record now says
  what its code does.
- **The archive is an input too.** After the night a bounded job reads the
  dispatch archive's Parquet files back into a table beside the live one.
- **Failure on purpose.** A worker is killed with SIGKILL at half-time, a
  producer resends two thousand position reports and another five hundred
  tablet events. Every topic, queue, stream, key and table is then checked
  against what the night's data says it must hold.
- **A compiled job beside the SQL.** MQTT and MongoDB are not reachable from
  clink's SQL, so the riders job is a C++ module, `jobs/riders`, built
  against clink's installed CMake package with the recipe it ships,
  `clink_add_job_module`, and submitted to the same Coordinator with `clink
  run --job`. It reads every phone's status from the rider's own topic,
  retained, keys it by rider, and keeps each rider's newest report by the
  phone's clock in MongoDB, replacing the rider's document; it sends a
  command to charge when a battery falls below 15 per cent, and not again
  until it has been back at 30. It runs beside the SQL jobs through the
  Worker kill and the Coordinator failover, which recovers it from the
  module its manifest keeps. clink's MQTT source acknowledges a message once
  it has read it, so a report lost with a killed Worker is not handed over
  again; the retained topics hand the job every rider's latest status when
  it subscribes again, and a battery stays low for several reports, so the
  store is exact and every low-battery episode is answered.
- **The operational scenes.** A standby Coordinator stands by from the
  start. At the goal in minute 67 the dispatcher's interval join, which
  pairs each order with the position reports of the riders in its zone and
  was declared rescalable when the job was submitted, is scaled to twice its
  parallelism while it runs, for the surge the goal brings. At minute 75 the
  gauges job is stopped at a savepoint, which commits its tail, and
  resubmitted from that savepoint as a new job. At minute 95 the leading
  Coordinator is killed; the standby takes the leader lock, binds its ports,
  and recovers all eighteen jobs from their checkpoints, the dispatcher at
  the parallelism it was scaled to, with the Workers following it through
  the HA directory. After the last goal the join is scaled back. The checks
  notice none of it.
- **The protocol, checked against its specification.** Every clink process
  records the steps of the exactly-once protocol it takes (checkpoint
  triggers, acknowledgements, completions, commits, restores): some 145,000
  events across the two Coordinators and the Workers of a run, under
  `state/run/traces`. clink's specification has a trace validator that
  accepts a job's run only if every step is one the specification allows in
  that state, and every event names its job, so `TRACE_CHECK=1` validates
  each of the night's twenty jobs on its own: the seventeen SQL jobs
  submitted, the riders job, the gauges job resumed from its savepoint, and
  the backfill. In the last full check seventeen were accepted, and three
  are outside the specification and skipped, each with its reason: the
  dispatcher was rescaled, which the specification does not model yet; the
  ledgers job commits through sinks of both families, a Kafka transaction
  beside two PostgreSQL tables, where the specification models one; and the
  resumed gauges job numbers its checkpoints on from its savepoint's, where
  the specification starts every job at 1. The check reads the run as it
  ended, with clink stopped, and takes ten to twenty-five minutes, four jobs
  at a time.

## How it works

```mermaid
flowchart LR
    subgraph sources["What the platform emits"]
        PG[("PostgreSQL<br/>orders")]
        RP["riders.positions"]
        MY[("MySQL<br/>menus")]
        KE["kitchen.events"]
        MF["the match feed<br/>HTTP"]
        DV["riders/+/status<br/>MQTT, retained"]
    end
    subgraph clink["clink: nineteen jobs"]
        O["orders<br/>change stream, exactly once"]
        P["pricing<br/>5 min windows, every minute"]
        Q["quotes<br/>the price when placed"]
        D["dispatch<br/>nearest rider or escalated"]
        L["ledgers<br/>payouts and credits, exactly once"]
        A["analytics<br/>counts and running spend"]
        R["archive"]
        M["menus<br/>change stream, exactly once"]
        B["bridge<br/>onto the tablet bus"]
        K["kitchen<br/>three bounces in a row"]
        OPS["ops<br/>escalations and alerts"]
        T["tracker<br/>latest state, by version"]
        S["search"]
        MT["match<br/>the feed, and each goal's orders"]
        LK["lake"]
        TM["telemetry"]
        G["gauges"]
        BF["backfill<br/>bounded, after the night"]
        RJ["riders, C++<br/>latest status, low battery"]
    end
    OC["orders.changes"]
    PM["pricing.multipliers"]
    OP["orders.priced"]
    DD["dispatch.decisions"]
    LP["ledger.payouts"]
    N4[("Neo4j<br/>city graph")]
    PGL[("PostgreSQL<br/>payouts, credits,<br/>zone_pricing")]
    CH[("ClickHouse")]
    PQ[("Parquet")]
    MC["menus.changes"]
    PK[("Pulsar<br/>kitchen.events")]
    KA["kitchen.alerts"]
    RMQ[("RabbitMQ<br/>ops.escalations")]
    PP[("Pulsar<br/>partners.escalations")]
    NT[("NATS<br/>ops.alerts")]
    RD[("Redis<br/>order:, rider-of:")]
    CS[("Cassandra<br/>order_history")]
    ME["match.events"]
    OS[("OpenSearch<br/>menus")]
    DL[("Delta<br/>orders.changes")]
    IX[("InfluxDB<br/>rider_position")]
    PGW[("Pushgateway<br/>zone multipliers")]
    PR[("Prometheus<br/>and Grafana")]
    MG[("MongoDB<br/>riderapp.riders")]
    RC["riders/commands<br/>MQTT"]
    PG --> O --> OC
    OC --> P
    RP --> P --> PM
    P --> PGL
    OC --> Q
    PM --> Q --> OP
    OP --> D
    RP --> D
    N4 -.-> D
    D --> DD
    OC --> L
    DD --> L --> PGL
    L --> LP
    OC --> A
    DD --> A
    PM --> A
    LP --> A --> CH
    OC --> R
    RP --> R
    DD --> R --> PQ
    MY --> M --> MC
    KE --> B --> PK --> K --> KA
    DD --> OPS
    KA --> OPS
    OPS --> RMQ
    OPS --> PP
    OPS --> NT
    OC --> T
    DD --> T
    T --> RD
    T --> CS
    MC --> S --> OS
    MF --> MT --> ME
    ME --> MT
    OC --> MT --> CH
    OC --> LK --> DL
    RP --> TM --> IX
    PM --> G --> PGW --> PR
    PQ --> BF --> CH
    DV --> RJ --> MG
    RJ --> RC
```

| Job | Reads | Writes | Guarantee |
| --- | --- | --- | --- |
| `orders` | the order table's replication stream | `orders.changes` | exactly once |
| `pricing` | `orders.changes`, `riders.positions`; its own `pricing.multipliers` for the table | `pricing.multipliers`; `zone_pricing` in PostgreSQL | exactly once; upsert |
| `quotes` | `orders.changes`, `pricing.multipliers` | `orders.priced` | exactly once |
| `dispatch` | `orders.priced`, `riders.positions`, the Neo4j graph | `dispatch.decisions` | exactly once |
| `ledgers` | `orders.changes`, `dispatch.decisions` | `payouts` and `credits` in PostgreSQL; `ledger.payouts` | exactly once |
| `analytics` | the four topics above | five ClickHouse tables, one of them each customer's running spend | at least once |
| `archive` | `orders.changes`, `riders.positions`, `dispatch.decisions` | three Parquet directories | exactly once |
| `menus` | the menu table's binary log, snapshot first | `menus.changes` | exactly once |
| `bridge` | `kitchen.events` | the Pulsar topic `kitchen.events` | at least once |
| `kitchen` | the Pulsar topic `kitchen.events` | `kitchen.alerts` | at least once |
| `ops` | `dispatch.decisions`, `kitchen.alerts` | the RabbitMQ queue `ops.escalations`, the Pulsar topic `partners.escalations`, the NATS subject `ops.alerts` | at least once |
| `tracker` | `orders.changes`, `dispatch.decisions` | the Redis keys `order:` and `rider-of:`; `order_history` in Cassandra | upsert; at least once by key |
| `search` | `menus.changes` | the OpenSearch index `menus`, one document per change | at least once, by document |
| `match` | the match feed over HTTP, `match.events`, `orders.changes` | `match.events`; `match_timeline` and `goal_orders` in ClickHouse | exactly once; at least once |
| `lake` | `orders.changes` | the Delta table `lake/orders.changes` | at least once |
| `telemetry` | `riders.positions` | the InfluxDB measurement `rider_position` | at least once, one point per series and time |
| `gauges` | `pricing.multipliers` | the Pushgateway gauge `halftime_zone_multiplier_pct`, scraped by Prometheus | at least once |
| `backfill` | the dispatch archive's Parquet files, after the night | `dispatch_decisions_backfill` in ClickHouse | at least once; bounded |
| `riders` (C++, `jobs/riders`) | the riders' phones over MQTT, `riders/+/status` | the MongoDB collection `riderapp.riders`, one document per rider; the MQTT topic `riders/commands` | upsert by rider, newest by the phone's clock; at least once |

Each job is one SQL file under `pipeline/`, submitted to a Coordinator and
two Workers. The same files run embedded with `clink run`.

### The rules

**Pricing** (`pipeline/pricing.sql`). For every zone, a five-minute window
sliding every minute counts the orders placed in it and the riders who
reported from it. The multiplier is 1.00x below one order per rider, 1.20x
from one, 1.50x from two, 1.80x from three, and 2.00x when no rider reported
at all. Each window is published once and kept as the zone's current price.

**Quotes** (`pipeline/quotes.sql`). An order is priced by the window that
closed in the minute it was placed, the latest price the zone had; a zone
with nothing to show for five minutes is priced at 1.00x. The quote carries
`decide_at`, two minutes after the order, which is when the dispatcher stops
waiting for riders to report in.

**Dispatch** (`pipeline/dispatch.sql`). The graph gives the restaurant's
position, its zone and the zones that touch it. Every report from those
zones between three minutes before the order and two minutes after it, and
within 1,500 m of the restaurant, is a candidate; the nearest wins, the lower
rider id on a tie, and an order with no candidate is escalated. The decision
is a window over the join, closed at `decide_at`, so every report that could
count has arrived before the rider is chosen.

**Ledgers** (`pipeline/ledgers.sql`). A payout is the delivery fee at the
quoted multiplier, recorded when the order store says the order was
delivered, to the rider the dispatcher chose. The delivery and the decision
meet in a join keyed by the order that keeps each side for ten minutes of
wall-clock time, so a dispatcher running late still finds the delivery
waiting. A credit is a fifth of the basket when a delivery came more than
ten minutes after the promise, or the basket when the order was cancelled.

**Analytics** (`pipeline/analytics.sql`). Every change, decision, pricing
window and payout lands in ClickHouse as a row. Each customer's running
spend is clink's event-time window over the order stream, which arrives in
the order the order store committed it. A rider's running pay is a ClickHouse
view over the payout rows instead: a payout is recorded when the later of the
delivery and the dispatch decision arrives, so payouts do not arrive in
delivery order, and an event-time running total would have to drop the ones
that arrive behind its watermark.

### Why each hop is a topic

The dispatcher could read the order table and compute its own quote. It does
not, because the quote is a fact the platform has already told the customer,
and because a job that reads only committed records of the job before it can
be restarted on its own, from its own checkpoint, without re-deciding what
an earlier job decided. Each topic written exactly once is a boundary a
restore cannot cross twice.

## Running it

You need Docker, Python 3, clink 0.13.0 or later (the `clink` and
`clink_node` binaries), and for the riders job CMake and a C++23
compiler: `run.sh`
builds `jobs/riders` against the clink checkout in `CLINK_SRC` (default
`../clink`), whose `build/` it installs into `.clink/prefix`, or against an
installed clink named by `CLINK_PREFIX`. Nothing else is installed on the
host: the producers and the checks run each system's own tools inside its
container, Kafka's in the broker's, `psql` in PostgreSQL's,
`clickhouse-client` in ClickHouse's, `mysql`, `pulsar-client`, `redis-cli`,
`cqlsh`, `mongosh` and `mosquitto_sub` in theirs, the `nats` command line in
a throwaway container on the compose network, and RabbitMQ, OpenSearch,
InfluxDB and the Pushgateway over HTTP; the riders' phones publish with a
few lines of Python of their own, `tools/mqtt.py`. The seventeen containers
want about eight gigabytes of memory between them; Kafka, Cassandra,
ClickHouse, OpenSearch and Pulsar take most of it.

```bash
CLINK_BIN_DIR=/path/to/clink/build ./run.sh
```

`run.sh` generates the night, starts every system from empty, starts the
match feed and clink, submits the seventeen SQL jobs and the riders job,
plays the night at thirty times real time (six minutes), kills Worker 1 at
half-time and starts it again, resends a batch of reports and a batch of
tablet events, runs the backfill, and checks every store. `SCENES=0` leaves
out the standby Coordinator, the savepoint and the failover; otherwise they
are part of every run. `SPEED=10` plays it slower; `CLEANUP=1` removes
everything at the end. `CAPTURE_JOBS=kitchen` records the kitchen job with
clink's flight recorder, for replaying offline afterwards (see "Dissecting
it"); any of the jobs can be named. `DISPATCH_PARALLELISM=3` runs the
dispatcher as three subtasks; every job otherwise runs as one. The stack
otherwise stays
up: Neo4j Browser is at http://localhost:7474, RabbitMQ's management UI at
http://localhost:15672, Grafana at http://localhost:23000 (admin / halftime,
with the half-time dashboard provisioned from `deploy/grafana`), Prometheus
at http://localhost:29090, the Coordinator at http://localhost:8081, and
ClickHouse answers on port 29000. Every one of them listens on this
machine's loopback address only: the systems take demo passwords, set in
`docker-compose.yml`, and several none at all, so none is for a network.

## Running it on a rig

The same pipeline scales out on Hetzner Cloud, one host per system group
and as many Workers as the project's quota allows, with the scripts under
`rig/`. Nothing in them has been run against a real project yet: the dry
runs are what this round delivers, and the first real run is a decision
with a bill.

```bash
RUN_ID=final-20261101 ./rig/provision.sh --dry      # the plan and the hourly price, nothing created
RUN_ID=final-20261101 ./rig/run.sh --dry            # every step and command of a run, nothing connected
RUN_ID=final-20261101 HT_HCLOUD_CONTEXT=<context> ./rig/provision.sh
RUN_ID=final-20261101 CELLS=12 RIDERS_PER_CELL=300 WORKERS=3 SLOTS=32 FEEDERS=4 ./rig/run.sh
./rig/teardown.sh final-20261101 && ./rig/teardown.sh --check
```

`provision.sh` creates a private network, an ops host that exports the
shared state over NFS, two Coordinator hosts, `WORKERS` Worker hosts and
one host each for Kafka, the stores (PostgreSQL, Neo4j, ClickHouse, MySQL),
the messaging systems (Pulsar, RabbitMQ, NATS, Redis, Cassandra) and search
and telemetry (OpenSearch, InfluxDB), every resource labelled with the run
id, and a firewall that opens ssh, the Coordinators' HTTP ports and the
dashboards to the operator's address only. Its dry run prices the plan from
Hetzner's own list: the default of three shared-core Workers comes to about
EUR 0.77 an hour. `run.sh` does over ssh what the local `run.sh` does on one
machine: the role compose files under `rig/roles` start each system group
on its host advertising its private address, clink runs from the runtime
image (`CLINK_IMAGE`) with the HA directory, the checkpoints and the
protocol trace on the shared state, the night is generated at the chosen
scale on the ops host and played by `FEEDERS` processes per stream, a Worker
container is killed at half-time, and the checks run on the ops host against
every system by address (`tools/hosts.py`, `HT_MODE=rig`). The keyed jobs
run at `PARALLELISM` (default a quarter of the rig's slots); the change
streams, the bridge and the feed poller run as one subtask, as their
sources are single readers. `teardown.sh` removes everything by label, and
its `--check` is the end of every session.

## What the checks prove

`tools/check_run.py` compares every store with `tools/reference.py`, a model
of the same rules computed from the generated night alone, in event time:

| Store | Held to |
| --- | --- |
| `orders.changes` | every change the order store committed, once |
| `pricing.multipliers`, `zone_pricing` | every window that closed, once, with the values the model computes; each zone at its latest window |
| `orders.priced` | every order, once, under the model's multiplier |
| `dispatch.decisions` | every order, once, with the model's rider, candidate count and score |
| `payouts`, `credits`, `ledger.payouts` | every payout and credit, once, to the cent |
| the ClickHouse tables | every row at least once, the copies counted; each customer's spend as the model computes it; each rider's total as the view computes it from the rows |
| the Parquet archives | every record of each stream, once |
| the Neo4j graph | the generated cities, zones and restaurants |

`tools/check_round2.py`, `tools/check_round3.py` and `tools/check_riders.py` do
the same for the systems around the core:

| Store | Held to |
| --- | --- |
| `menus.changes`, the MySQL table | the menu store's rows as the night began and every change, once, with the values written; each item at its final state |
| the Pulsar topic `kitchen.events` | every tablet event at least once, the copies counted |
| `kitchen.alerts` | every run of three bounces at a restaurant, at least once, naming the model's three orders |
| the RabbitMQ queue, the Pulsar topic `partners.escalations` | every escalated order at least once |
| the NATS stream `ALERTS` | every alert at least once |
| the Redis keys | every order at its final state; every assigned order's rider |
| the Cassandra table | every change to every order at least once, the copies counted |
| the OpenSearch index | every menu change as one document, with the values written |
| `match.events`, `match_timeline`, `goal_orders` | every event once on the topic and at least once in the timeline; every order placed in the two minutes from a goal, against that goal |
| the Delta table | every change at least once, read back by ClickHouse from the files the log commits |
| the InfluxDB measurement | one point per report, exactly: the store keeps one value per series and time |
| the Pushgateway | each zone's multiplier equal to the topic's row for the window it names, at or near the latest |
| `dispatch_decisions_backfill` | every decision the archive holds, as the topic has it |
| the MongoDB collection `riderapp.riders` | one document per rider, each rider's latest status as the model says (`tools/check_riders.py`) |
| the MQTT topic `riders/commands` | a command to charge for every low-battery episode, at least once, each answering a report below 15 per cent with the battery the model gives |
| the protocol trace | recorded for every process; with `TRACE_CHECK=1` and `CLINK_SRC` set to a clink checkout, each job's run validated against the exactly-once specification by `scripts/formal-check.sh --trace`: accepted, or skipped as outside it with the reason |

The night ends, and a stream does not. The checks require every row the
stream's watermark could have reached: the pricing windows up to the last
minute, the decisions whose window closed before the quotes stopped, and the
running spend up to the lowest partition's last delivery. A row past that
point is held to the same standard when it appears, and the check reports
how many were still waiting. In the run above, every one of the 98 orders
still inside their dispatch window when the quotes stopped was decided once
the idle timeout released them, and seven of 5,291 spend rows were still
waiting on a partition that had ended early.

### Idle partitions

Every topic here but one has three partitions and nothing carries a key, so
each job's transactional sink puts a burst of records, such as one minute's
quotes, on whichever partition its producer is on. (`kitchen.events` has one
partition: the tablet vendor's bus is ordered, and the bridge keeps it so.) A partition can go six
minutes of match time without a record while the others move on. clink
tracks a watermark per partition and takes the lowest, which is what keeps a
quiet partition's records from arriving late, so without idleness handling
the dispatcher's watermark would freeze at the quiet partition's last record
and the orders behind it would never be decided. Every table therefore sets
`idle_timeout_ms` to ninety seconds: a partition that has been quiet that
long is set aside until it speaks again. The trade-off is at start-up, where
a derived topic is empty until the job before it has committed its first
rows; ninety seconds is several times longer than that takes here, and a
shorter setting cost one quote its multiplier in an earlier run.

Idleness is detected per scan, on each one's own clock, and the end of the
night showed what that means for a statement set. The price table was first
kept by a second statement of the pricing job that computed the windows
again from the same two sources. Recorded with the flight recorder, the two
statements' unions saw the order stream and the riders' reports go quiet in
the same two seconds but in a different order: one took the order stream's
last watermark, 190, when the riders went quiet first, and closed six
windows more; the other, with the order stream quiet first, kept the
riders' 158 and closed none. Both are what idleness means, and which comes
first is a race between four probes. The table is now kept from the
published rows, the latest window per zone, so it is a function of the
topic and ends where the topic ends.

## What it costs

`tools/footprint.py` samples clink's processes from their own `/metrics` and
every container from `docker stats` through a run, and the run ends with the
figures. On one machine at thirty times real time, with the scenes, the
seventeen jobs and the backfill ran in four clink processes whose peaks
summed to 1.0 GiB: the Coordinator 43 MiB, its standby 39 MiB, the two
Workers 501 MiB and 448 MiB (means of 232 MiB and 209 MiB), at 21 and 19
per cent of a core on average. The fifteen systems they fed peaked at
8.2 GiB between them; Kafka, Cassandra, ClickHouse, OpenSearch and Pulsar
each above a gigabyte. The clink binaries were a host debug build; the runtime image is
a release build and does better on both counts. A run without the scenes
(`SCENES=0`) on an otherwise quiet machine gives the cleaner figure.

## What it found

A pipeline this wide is a test of the engine, and the first runs of each
round were. Each of these was reproduced in clink's own tests and fixed
there before the round was called done:

- A keyed row's state was filed under one key group and its rows routed
  by another, so a Worker kill at parallelism three lost two thirds of the
  dispatch join's buffered reports and decided 378 orders with fewer riders
  than it had seen.
- A job over a source its broker redelivers (Pulsar, RabbitMQ, NATS) was
  reported as losing what was in flight and refused even an `at_least_once`
  declaration, where at least once is exactly what such a source gives.
- In a statement set, a Top-N statement left a flag set that made the next
  statement drop its `WHERE`: the tracker wrote every order's rider instead
  of the assigned ones'.
- `MATCH_RECOGNIZE` matched `b{3}` across rows that were not bounces, so 269
  kitchens were reported overloaded where the night held 59 runs of three.
- A numeric value a text-protocol source delivered as `"2000"` stayed a
  string in a `BIGINT` column, through Kafka and into OpenSearch.
- The Redis upsert sink wrote `order:"lisbon-o00940"\x1f` where its page
  promised `order:lisbon-o00940`.
- The HTTP polling source's capability record said it kept no cursor and
  sat outside checkpointing, while its code checkpoints the cursor and
  re-polls from it; the record barred a job over the feed from declaring
  any guarantee.
- `MATCH_RECOGNIZE` partitioned by the routing token the keyer stamps on a
  row, one of 128, rather than by the partition key, so two restaurants
  whose keys share a key group were one sequence of events to the pattern:
  a quiet kitchen's accepted orders ended three of a busy one's runs of
  bounces, the same three in every run. The restarts those alerts had been
  put down to had nothing to do with it; the flight recorder showed both
  restaurants' rows under one key at the operator's door.
- A job submitted to a Coordinator could not arm the flight recorder: the
  flag reached the embedded path only, and a takeover rebuilt a job without
  it. Both now carry it, which is what made the trace above possible.
- The sinks' own events in the protocol trace named no job, so a run of
  seventeen jobs could not be checked against the exactly-once
  specification at all. They now name it, and the check validates each
  job of a run on its own.
- The Pulsar source could replay a restart only by broker redelivery,
  which hands a restarted consumer its unacknowledged messages after newer
  ones. It now takes an exclusive subscription that records its position
  at each checkpoint and seeks back to it, which is what lets the kitchen
  watch run with a minute of lateness.
- SQL jobs declared no rescale bounds, so the dispatcher could not be
  resized while it ran. A SQL job now opts in with `--max-parallelism`,
  which declares the keyed stages whose output is inserts only: a stage that
  revises what it emitted would have a key's updates reordered on their way
  to the sink once the edge out of it is round-robin.
- The clink test written for that scene found three defects in the rescale
  itself, all now fixed: a stage chained with the stages it fed split from
  them, and the replan restored those from a block they never wrote; after a
  rescale no commit of a job with a Kafka transactional sink was confirmed
  again, the Coordinator still waiting on the sinks' old places; and a
  Coordinator failover after a rescale planned the job as submitted against
  checkpoints at the new layout, and left it stopped until resubmitted.
- clink's MQTT sink talked to the broker only when it had something to
  publish, so with nothing to send it missed its keepalive, the broker
  dropped it, and its next flush restarted the riders job: the commands sink
  was dropped in the first minutes of the night, before any rider's battery
  ran low. It now keeps its connection while idle, and reconnects one that
  dropped with nothing in flight.
- Building the riders job turned up two gaps in submitting a compiled job. A
  job module that registered its own operators had to register clink's
  built-in channel types itself first, or its build failed with "T not
  registered"; the macro that declares a job now registers them before the
  job's code runs. And `clink run --job` exited with a failure on every
  submission that did not wait for its job to complete, which a streaming job
  never does; it now exits 0 once the Coordinator has accepted the job.

Still open, and noted here rather than hidden: MQTT and MongoDB are reached
through clink's C++ API, and not yet from its SQL.

## Dissecting it

Everything the run does is in plain files, and clink shows its working.
The systems stay up after a run; the clink processes stop when `run.sh`
ends, so the Coordinator's API is read while the night plays (about six
minutes, with the time between the scenes to spare), and the files
afterwards:

- The riders job, the C++ one: its source is `jobs/riders/riders.cpp`, the
  module `run.sh` built is `.clink/riders/riders.so`, the rider app's store
  is `docker compose exec mongo mongosh riderapp --quiet --eval
  'db.riders.find().limit(3)'` and the commands it sent are in
  `state/run/commands.txt`.
- A job's plan, without running it:
  `clink run pipeline/dispatch.sql --explain` prints the logical plan the
  planner built, join shape and all.
- A job's guarantee, as the planner computed it from its sources and sinks:
  `curl -s localhost:8081/api/v1/jobs/4 | python3 -m json.tool` shows
  `delivery_guarantee`, what limits it and the reasoning
  (`guarantee_reasons`); try the kitchen job, held to at least once, beside
  the dispatcher, exactly once. `state/run/jobs.tsv` lists each job's id,
  which follows the submission order in `run.sh`. After the failover the
  Coordinator is on port 8084.
- What each operator received, emitted and dropped:
  `curl -s localhost:8081/api/v1/jobs/4/operators`. `run.sh` keeps the
  kitchen job's counters from before the failover and from the end under
  `state/run/kitchen-operators-*.json`.
- The state inside a checkpoint: `clink state-cat
  --file state/run/checkpoints/_state/10/v1/1/checkpoint-<n>.snap` dumps the
  kitchen job's `distinct` set (one entry per tablet event) and the pattern
  operator's held rows (`__cep_held__`), one entry per restaurant, under
  the restaurant's id; `clink state-diff` compares two checkpoints.
- The flight recorder: `CAPTURE_JOBS=kitchen ./run.sh` records what every
  operator of the kitchen job was handed, per checkpoint epoch, under
  `state/run/capture/kitchen/op-<id>/subtask-<n>/epoch-<checkpoint>.cap`
  (each operator's `op.json` says which it is), and copies the job's
  snapshots to `state-before-failover` just before the Coordinator is
  killed, since the checkpoint directory keeps only the newest. `clink
  capture-cat --dir state/run/capture/kitchen` lists the epochs, `--file`
  prints one, rows and watermarks in the order the operator saw them, and
  `clink replay --capture-dir state/run/capture/kitchen --checkpoint-dir
  state/run/capture/kitchen/state-before-failover/v1 --op <id> --epoch <n>
  --state-from <m> --verify`, with `m` the checkpoint the standby's log says
  the job resumed from and `n` the first epoch the recorder wrote after it,
  rebuilds the pattern operator from the restore point, feeds it that epoch
  and prints what it emits, twice, to show the two runs identical. That is
  how the kitchen defect below was found: the operator's own input, read
  back, showed two restaurants' rows under one key.
- The model the checks compare against: `python3 tools/reference.py`
  prints its counts, and `tools/reference.py` mirrors each job's rules in
  plain Python, one function per job, so a check line can be traced to the
  rule it holds.
- The protocol trace: `state/run/traces/*.ndjson`, one JSON object per
  protocol step per process, in the vocabulary of `formal/trace/events.txt`
  in the clink repository.
- The dashboards: Grafana at http://localhost:23000 shows clink's own
  metrics (memory, CPU, checkpoints, subtasks per Worker) beside the zone
  multipliers the gauges job pushes; Prometheus at http://localhost:29090
  has the raw series.
- The systems themselves: every container keeps its native tools, as the
  checks use them (`docker compose exec kafka ...`, `psql`,
  `clickhouse-client`, `cqlsh`, `redis-cli`, `pulsar-client`), and
  `tools/hosts.py` is the one place that knows where each one is.

## What is generated

`tools/world.py` draws the night from one seed: three cities of nine zones
each, 36 restaurants, 1,200 customers, 216 riders and about 5,500 orders,
some 21,000 changes to them and 190,000 position reports. Orders arrive at
rates set by the match: fewer during play, a surge for two minutes after
each goal, three times the rate at half-time, a lull during penalties and a
rush after the whistle. Half the riders stop for six to twelve minutes after
a goal or for the penalties. Riders work near a restaurant, the busier the
likelier, so a quiet restaurant can find no rider within reach. The kitchens
and the menus are drawn apart from the rest, so they can change without a
single order or report changing: 288 menu items and 178 changes to them
through the night, and some 11,600 tablet events, of which about 800 are
bounces, clustered at the busiest kitchens in the busiest minutes, so that
59 runs of three reach the floor. The match feed is ten events: kick-off,
four goals, half-time, the second half, extra time, the penalties and the
final whistle, each at the start of its minute, which is when the orders
surge. The riders' phones are drawn apart too: each reports every two
minutes, 19,440 reports in all, with a battery that drains while the rider
rides and charges on a break, and 110 times a battery falls below 15 per
cent.

## Status

Round 1: the core pipeline, on one machine. Round 2: the systems around it,
the legacy menu system (MySQL change data capture), the tablets' bus
(Pulsar), the desk, the partners and the floor (RabbitMQ, Pulsar, NATS) and
the tracker (Redis, Cassandra). Round 3: the match feed (HTTP polling),
search (OpenSearch), telemetry (InfluxDB), the lakehouse (Delta), the gauges
(Pushgateway, Prometheus, Grafana) and the backfill from the archive. Round
4: the operational scenes, a savepoint stop and resume, a Coordinator
failover, and the protocol trace checked against the specification. Round 5:
the footprint sampler, so a run ends with what clink and each system cost.
Round 6: the rig scripts, dry-run only. Since then: the flight recorder, the
trace checked job by job, the bus replayed in order, the dispatcher resized
live during a surge, and the riders job, built on clink's C++ API, for the
riders' phones (MQTT) and the rider app's store (MongoDB).

## Licence

Apache License 2.0. See `LICENSE` and `NOTICE`.
