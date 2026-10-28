#!/usr/bin/env bash
# Run half-time end to end, a Worker kill included, and check every store it
# writes against what the night's data says.
#
#   CLINK_BIN_DIR=<clink build dir> ./run.sh   clink from a local build
#   SPEED=30 ./run.sh                          the night at thirty times real time
#   CLEANUP=1 ... ./run.sh                     and remove everything at the end
#
# Steps: generate the night (tools/world.py); start the broker, the order
# store, the city graph and the analytics store from empty and apply their
# schemas; start a Coordinator and two Workers; submit the seven jobs
# (pipeline/orders.sql, pricing.sql, quotes.sql, dispatch.sql, ledgers.sql,
# analytics.sql, archive.sql); play the night at SPEED times real time, the
# riders' reports into Kafka and the orders into the order store, killing a
# Worker with SIGKILL at half-time and starting it again; resend a batch of
# reports, as a producer that retried would; then check every store against
# the reference model.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

: "${CLINK_BIN_DIR:?set CLINK_BIN_DIR to a clink build directory (clink_node and clink)}"
BIN="$(cd "${CLINK_BIN_DIR}" && pwd)"
for b in clink_node clink; do
  [ -x "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
done
KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-29092}"
POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-25432}"
NEO4J_HTTP_PORT="${NEO4J_HTTP_PORT:-7474}"
CLICKHOUSE_HOST_PORT="${CLICKHOUSE_HOST_PORT:-29000}"
CLINK_HTTP_PORT="${CLINK_HTTP_PORT:-8081}"
SPEED="${SPEED:-30}"
export KAFKA_HOST_PORT POSTGRES_HOST_PORT NEO4J_HTTP_PORT CLICKHOUSE_HOST_PORT
# Read by the Workers for the dispatch job's lookup table.
export NEO4J_PASSWORD=half-time-pw
STATE="${ROOT}/state/run"

say() { printf '\n==> %s\n' "$*"; }

pids=()
worker1_pid=""
cleanup() {
  local pid
  for pid in "${pids[@]}" ${worker1_pid}; do
    kill "${pid}" 2>/dev/null || true
  done
  # A process that has not stopped after ten seconds is killed.
  for _ in $(seq 1 10); do
    alive=0
    for pid in "${pids[@]}" ${worker1_pid}; do
      kill -0 "${pid}" 2>/dev/null && alive=1
    done
    [ "${alive}" = 0 ] && break
    sleep 1
  done
  for pid in "${pids[@]}" ${worker1_pid}; do
    kill -9 "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [ "${CLEANUP:-0}" = 1 ]; then
    docker compose down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

await_http() {  # <url> <what>
  for _ in $(seq 1 60); do
    curl -fsS "$1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "run.sh: $2 did not come up" >&2
  exit 1
}

start_worker() {  # <n>
  "${BIN}/clink_node" --role=worker --id="worker-$1" --coordinator-host=127.0.0.1 \
    --coordinator-port=16123 --port=$((16123 + $1)) --data-host=127.0.0.1 --slots=96 \
    --http-port=$((18081 + $1)) >>"${STATE}/worker-$1.log" 2>&1 &
  last_pid=$!
  await_http "http://127.0.0.1:$((18081 + $1))/api/v1/health" "Worker $1"
}

psql_q() {  # <sql>: one value
  docker compose exec -T postgres psql -At -U halftime -d halftime -c "$1" 2>/dev/null || echo ""
}

say "The night, generated"
python3 -I tools/world.py --out state/world

say "Starting the broker, the order store, the city graph and the analytics store, from empty"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf "${ROOT}/state/archive" && mkdir -p "${ROOT}/state/archive"
docker compose up -d --wait
docker compose exec -T postgres psql -q -v ON_ERROR_STOP=1 -U halftime -d halftime -f - <postgres/schema.sql
echo "order store: postgres/schema.sql applied"
docker compose exec -T clickhouse clickhouse-client --user halftime --password halftime \
  --multiquery <clickhouse/schema.sql
echo "analytics store: clickhouse/schema.sql applied"
for i in $(seq 1 60); do
  python3 -I tools/load_graph.py --reset 2>/dev/null && break
  [ "${i}" = 60 ] && { echo "run.sh: Neo4j's Query API did not answer" >&2; exit 1; }
  sleep 2
done

say "Starting a Coordinator and two Workers, and submitting the seven jobs"
rm -rf "${STATE}" && mkdir -p "${STATE}"
"${BIN}/clink_node" --role=coordinator --bind-host=127.0.0.1 --port=16123 \
  --http-port="${CLINK_HTTP_PORT}" --ha-dir="${STATE}/ha" >"${STATE}/coordinator.log" 2>&1 &
pids+=($!)
await_http "http://127.0.0.1:${CLINK_HTTP_PORT}/api/v1/health" "the Coordinator"
start_worker 1
worker1_pid="${last_pid}"
start_worker 2
pids+=("${last_pid}")
for job in orders pricing quotes dispatch ledgers analytics archive; do
  # The same SQL, reaching the services through their host ports.
  sed -e "s#kafka:9092#localhost:${KAFKA_HOST_PORT}#g" \
      -e "s#http://neo4j:7474#http://localhost:${NEO4J_HTTP_PORT}#g" \
      -e "s#host=postgres port=5432#host=localhost port=${POSTGRES_HOST_PORT}#g" \
      -e "s#host *= *'clickhouse'#host = 'localhost'#g" \
      -e "s#port *= *'9000'#port = '${CLICKHOUSE_HOST_PORT}'#g" \
      -e "s#'/archive/#'${ROOT}/state/archive/#g" \
      "pipeline/${job}.sql" >"${STATE}/${job}.sql"
  "${BIN}/clink" run "${STATE}/${job}.sql" --coordinator-host=127.0.0.1 \
    --coordinator-port="${CLINK_HTTP_PORT}" --name="${job}" \
    --checkpoint-dir="${STATE}/checkpoints" --checkpoint-interval-ms=2000
done
# The order stream is reading once its replication slot is active.
for _ in $(seq 1 60); do
  [ "$(psql_q "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'halftime_orders' AND active")" = 1 ] && break
  sleep 1
done
echo "order stream: replication slot halftime_orders is active"

say "Playing the night at ${SPEED}x, and killing Worker 1 at half-time"
start_at="$(python3 -c 'import time; print(time.time() + 3)')"
python3 -I tools/feed_pings.py --speed "${SPEED}" --start-at "${start_at}" &
pings_pid=$!
python3 -I tools/feed_orders.py --speed "${SPEED}" --start-at "${start_at}" &
orders_pid=$!
# Half-time starts at minute 45; the kill lands in the thick of it.
for _ in $(seq 1 1200); do
  m="$(cat "${STATE}/orders.progress" 2>/dev/null || echo -99)"
  [ "${m}" -ge 50 ] && break
  sleep 0.5
done
echo "match minute ${m}: orders in the order store: $(psql_q 'SELECT count(*) FROM orders')"
kill -9 "${worker1_pid}"
wait "${worker1_pid}" 2>/dev/null || true
sleep 2
start_worker 1
worker1_pid="${last_pid}"
wait "${pings_pid}"
wait "${orders_pid}"

say "A producer resends a batch of reports it had already delivered"
python3 -I tools/feed_pings.py --resend 2000

say "Checking every store against the reference model"
rc=0
python3 -I tools/check_run.py --wait 300 || rc=1
echo
echo "Neo4j Browser: http://localhost:${NEO4J_HTTP_PORT} (neo4j / ${NEO4J_PASSWORD})."
echo "Coordinator: http://localhost:${CLINK_HTTP_PORT}. Logs: ${STATE}."
exit "${rc}"
