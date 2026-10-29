#!/usr/bin/env bash
# Run half-time end to end, a Worker kill included, and check every store it
# writes against what the night's data says.
#
#   CLINK_BIN_DIR=<clink build dir> ./run.sh   clink from a local build (clink and clink_node)
#   SPEED=30 ./run.sh                          the night at thirty times real time
#   DISPATCH_PARALLELISM=3 ./run.sh            the dispatcher three ways (see README)
#   SCENES=0 ./run.sh                          the night without the operational scenes
#   CLINK_SRC=<clink checkout> ./run.sh        where scripts/formal-check.sh is (default ../clink)
#   CLEANUP=1 ... ./run.sh                     and remove everything at the end
#
# Steps: generate the night (tools/world.py); start every system from empty
# (the broker, the order store, the city graph, the analytics store, the menu
# store, the tablet bus, the desk's queue, the alert bus, the tracker, the
# order history, search, the telemetry store, the gauges and the dashboards)
# and apply their schemas; start the match feed, a Coordinator and two
# Workers; submit the seventeen streaming jobs under pipeline/; play the
# night at SPEED times real time, the riders' reports and the tablets' events
# into Kafka, the orders into the order store and the menu changes into the
# menu store, killing a Worker with SIGKILL at half-time and starting it
# again; resend a batch of reports and a batch of tablet events, as producers
# that retried would; read the dispatch archive back with a bounded job; then
# check every store against the reference model.
#
# The operational scenes (SCENES=1, the default) run inside the night: a
# standby Coordinator stands by from the start; at minute 75 the gauges job
# is stopped at a savepoint and resubmitted from it; at minute 95 the leading
# Coordinator is killed and the standby takes over, with every job recovered
# from its checkpoints. Every clink process records its protocol trace under
# state/run/traces; TRACE_CHECK=1 hands it to the exactly-once
# specification's validator after the stores are checked (see the README for
# why that is opt-in today).
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

: "${CLINK_BIN_DIR:?set CLINK_BIN_DIR to a clink build directory (clink_node and clink)}"
BIN="$(cd "${CLINK_BIN_DIR}" && pwd)"
for b in clink_node clink clink_submit_sql; do
  [ -x "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
done
KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-29092}"
POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-25432}"
NEO4J_HTTP_PORT="${NEO4J_HTTP_PORT:-7474}"
CLICKHOUSE_HOST_PORT="${CLICKHOUSE_HOST_PORT:-29000}"
MYSQL_HOST_PORT="${MYSQL_HOST_PORT:-23306}"
RABBITMQ_HOST_PORT="${RABBITMQ_HOST_PORT:-5672}"
RABBITMQ_HTTP_PORT="${RABBITMQ_HTTP_PORT:-15672}"
NATS_HOST_PORT="${NATS_HOST_PORT:-4222}"
REDIS_HOST_PORT="${REDIS_HOST_PORT:-6379}"
OPENSEARCH_HOST_PORT="${OPENSEARCH_HOST_PORT:-29200}"
INFLUXDB_HOST_PORT="${INFLUXDB_HOST_PORT:-28086}"
PUSHGATEWAY_HOST_PORT="${PUSHGATEWAY_HOST_PORT:-29091}"
PROMETHEUS_HOST_PORT="${PROMETHEUS_HOST_PORT:-29090}"
GRAFANA_HOST_PORT="${GRAFANA_HOST_PORT:-23000}"
MATCH_FEED_PORT="${MATCH_FEED_PORT:-28090}"
CLINK_HTTP_PORT="${CLINK_HTTP_PORT:-8081}"
CLINK_STANDBY_HTTP_PORT="${CLINK_STANDBY_HTTP_PORT:-8084}"
SPEED="${SPEED:-30}"
DISPATCH_PARALLELISM="${DISPATCH_PARALLELISM:-1}"
SCENES="${SCENES:-1}"
CLINK_SRC="${CLINK_SRC:-${ROOT}/../clink}"
export KAFKA_HOST_PORT POSTGRES_HOST_PORT NEO4J_HTTP_PORT CLICKHOUSE_HOST_PORT
export MYSQL_HOST_PORT RABBITMQ_HOST_PORT RABBITMQ_HTTP_PORT NATS_HOST_PORT REDIS_HOST_PORT
export OPENSEARCH_HOST_PORT INFLUXDB_HOST_PORT PUSHGATEWAY_HOST_PORT PROMETHEUS_HOST_PORT GRAFANA_HOST_PORT
# Read by the Workers for the dispatch job's lookup table.
export NEO4J_PASSWORD=half-time-pw
STATE="${ROOT}/state/run"

say() { printf '\n==> %s\n' "$*"; }

pids=()
worker1_pid=""
coord_pid=""
cleanup() {
  local pid
  for pid in ${pids[@]+"${pids[@]}"} ${worker1_pid}; do
    kill "${pid}" 2>/dev/null || true
  done
  # A process that has not stopped after ten seconds is killed.
  for _ in $(seq 1 10); do
    alive=0
    for pid in ${pids[@]+"${pids[@]}"} ${worker1_pid}; do
      kill -0 "${pid}" 2>/dev/null && alive=1
    done
    [ "${alive}" = 0 ] && break
    sleep 1
  done
  for pid in ${pids[@]+"${pids[@]}"} ${worker1_pid}; do
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

# The leading Coordinator's ports: the first pair until the failover scene,
# the standby's after it. The HTTP port is what submissions use, the control
# port what stop, savepoint and rescale use.
coord_http() {
  curl -fsS "http://127.0.0.1:${CLINK_HTTP_PORT}/api/v1/health" >/dev/null 2>&1 \
    && echo "${CLINK_HTTP_PORT}" || echo "${CLINK_STANDBY_HTTP_PORT}"
}
coord_ctl() {
  [ "$(coord_http)" = "${CLINK_HTTP_PORT}" ] && echo 16123 || echo 16124
}

start_worker() {  # <n>
  # A Worker finds the leading Coordinator through the HA directory, so it
  # follows a takeover.
  "${BIN}/clink_node" --role=worker --id="worker-$1" --ha-dir="${STATE}/ha" \
    --port=$((16123 + $1)) --data-host=127.0.0.1 --slots=96 \
    --http-port=$((18081 + $1)) >>"${STATE}/worker-$1.log" 2>&1 &
  last_pid=$!
  await_http "http://127.0.0.1:$((18081 + $1))/api/v1/health" "Worker $1"
}

psql_q() {  # <sql>: one value
  docker compose exec -T postgres psql -At -U halftime -d halftime -c "$1" 2>/dev/null || echo ""
}

say "The night, generated"
python3 -I tools/world.py --out state/world

say "Starting every system from empty"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf "${ROOT}/state/archive" && mkdir -p "${ROOT}/state/archive"
# The long-lived services first, then the two one-shots, each run to its end:
# up --wait takes a one-shot that finishes while a slow service is still
# starting for a failure.
docker compose up -d --wait kafka postgres neo4j clickhouse mysql pulsar rabbitmq nats redis cassandra \
  opensearch influxdb pushgateway prometheus grafana
docker compose run --rm --no-deps -T kafka-init
docker compose run --rm --no-deps -T nats-init
docker compose exec -T postgres psql -q -v ON_ERROR_STOP=1 -U halftime -d halftime -f - <postgres/schema.sql
echo "order store: postgres/schema.sql applied"
docker compose exec -T clickhouse clickhouse-client --user halftime --password halftime \
  --multiquery <clickhouse/schema.sql
echo "analytics store: clickhouse/schema.sql applied"
docker compose exec -T -e MYSQL_PWD=halftime mysql mysql -u root halftime <mysql/schema.sql
echo "menu store: mysql/schema.sql applied"
python3 -I tools/feed_menus.py --initial
docker compose exec -T cassandra cqlsh <cassandra/schema.cql
echo "order history: cassandra/schema.cql applied"
python3 -I tools/init_messaging.py
for i in $(seq 1 60); do
  python3 -I tools/load_graph.py --reset 2>/dev/null && break
  [ "${i}" = 60 ] && { echo "run.sh: Neo4j's Query API did not answer" >&2; exit 1; }
  sleep 2
done

say "Starting the match feed, a Coordinator and two Workers, and submitting the seventeen jobs"
rm -rf "${STATE}" && mkdir -p "${STATE}/traces"
# Every clink process records its protocol trace here (see the end of the run).
export CLINK_PROTOCOL_TRACE_DIR="${STATE}/traces"
# The feeders and the match feed share one clock: the first record plays
# forty-five seconds from now, which is longer than the jobs take to submit.
start_at="$(python3 -c 'import time; print(time.time() + 45)')"
python3 -I tools/match_feed.py --port "${MATCH_FEED_PORT}" --speed "${SPEED}" --start-at "${start_at}" \
  >"${STATE}/match_feed.log" 2>&1 &
pids+=($!)
await_http "http://127.0.0.1:${MATCH_FEED_PORT}/health" "the match feed"
"${BIN}/clink_node" --role=coordinator --bind-host=127.0.0.1 --port=16123 \
  --http-port="${CLINK_HTTP_PORT}" --ha-dir="${STATE}/ha" >"${STATE}/coordinator.log" 2>&1 &
coord_pid=$!
pids+=("${coord_pid}")
await_http "http://127.0.0.1:${CLINK_HTTP_PORT}/api/v1/health" "the Coordinator"
if [ "${SCENES}" = 1 ]; then
  # The standby holds its ports closed until it wins the leader lock.
  "${BIN}/clink_node" --role=coordinator --bind-host=127.0.0.1 --port=16124 \
    --http-port="${CLINK_STANDBY_HTTP_PORT}" --ha-dir="${STATE}/ha" >"${STATE}/coordinator-standby.log" 2>&1 &
  pids+=($!)
  for _ in $(seq 1 30); do
    grep -q "standing by for leadership" "${STATE}/coordinator-standby.log" 2>/dev/null && break
    sleep 1
  done
  echo "a standby Coordinator is standing by for leadership"
fi
python3 -I tools/footprint.py --out "${STATE}/footprint.jsonl" >/dev/null 2>&1 &
pids+=($!)
footprint_pid=$!
start_worker 1
worker1_pid="${last_pid}"
start_worker 2
pids+=("${last_pid}")
for f in pipeline/*.sql; do
  job="$(basename "${f}" .sql)"
  # The same SQL, reaching the services through their host ports.
  sed -e "s#kafka:9092#localhost:${KAFKA_HOST_PORT}#g" \
      -e "s#http://neo4j:7474#http://localhost:${NEO4J_HTTP_PORT}#g" \
      -e "s#host=postgres port=5432#host=localhost port=${POSTGRES_HOST_PORT}#g" \
      -e "s#host *= *'clickhouse'#host = 'localhost'#g" \
      -e "s#port *= *'9000'#port = '${CLICKHOUSE_HOST_PORT}'#g" \
      -e "s#'/archive/#'${ROOT}/state/archive/#g" \
      -e "s#host *= *'mysql'#host = '127.0.0.1'#g" \
      -e "s#port *= *'3306'#port = '${MYSQL_HOST_PORT}'#g" \
      -e "s#pulsar://pulsar:6650#pulsar://localhost:6650#g" \
      -e "s#host *= *'rabbitmq'#host = 'localhost'#g" \
      -e "s#port *= *'5672'#port = '${RABBITMQ_HOST_PORT}'#g" \
      -e "s#nats://nats:4222#nats://localhost:${NATS_HOST_PORT}#g" \
      -e "s#host *= *'redis'#host = 'localhost'#g" \
      -e "s#port *= *'6379'#port = '${REDIS_HOST_PORT}'#g" \
      -e "s#contact_points *= *'cassandra'#contact_points = 'localhost'#g" \
      -e "s#http://opensearch:9200#http://localhost:${OPENSEARCH_HOST_PORT}#g" \
      -e "s#http://influxdb:8086#http://localhost:${INFLUXDB_HOST_PORT}#g" \
      -e "s#http://pushgateway:9091#http://localhost:${PUSHGATEWAY_HOST_PORT}#g" \
      -e "s#http://match-feed:28090#http://localhost:${MATCH_FEED_PORT}#g" \
      "${f}" >"${STATE}/${job}.sql"
done
for job in orders pricing quotes dispatch ledgers analytics archive menus bridge kitchen ops tracker \
           search match lake telemetry gauges; do
  p=1; [ "${job}" = dispatch ] && p="${DISPATCH_PARALLELISM}"
  submitted="$("${BIN}/clink" run "${STATE}/${job}.sql" --coordinator-host=127.0.0.1 \
    --coordinator-port="${CLINK_HTTP_PORT}" --name="${job}" --parallelism="${p}" \
    --checkpoint-dir="${STATE}/checkpoints" --checkpoint-interval-ms=2000)"
  echo "${submitted}"
  [ "${job}" = gauges ] && gauges_job="$(python3 -c 'import json, sys; print(json.loads(sys.argv[1])["job_id"])' "${submitted}")"
  [ "${job}" = kitchen ] && kitchen_job="$(python3 -c 'import json, sys; print(json.loads(sys.argv[1])["job_id"])' "${submitted}")"
done
# The kitchen job's per-operator counters, for the record: what each operator
# received, emitted and dropped, taken before the failover and at the end.
operators_of() {  # <job id> <file>
  curl -fsS "http://127.0.0.1:$(coord_http)/api/v1/jobs/$1/operators" >"$2" 2>/dev/null || true
}
# The order stream is reading once its replication slot is active.
for _ in $(seq 1 60); do
  [ "$(psql_q "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'halftime_orders' AND active")" = 1 ] && break
  sleep 1
done
echo "order stream: replication slot halftime_orders is active"
# The menu stream has announced its snapshot before the first change is made.
# (The MySQL client takes 'localhost' for a socket, hence 127.0.0.1 above.)
items="$(python3 -c 'import json; print(json.load(open("state/world/manifest.json"))["menu_items"])')"
deadline=$(( $(date +%s) + 120 ))
while :; do
  n="$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 \
         --topic menus.changes 2>/dev/null | awk -F: '{s += $NF} END {print s + 0}' || echo 0)"
  [ "${n:-0}" -ge "${items}" ] && break
  [ "$(date +%s)" -ge "${deadline}" ] && { echo "run.sh: the menu stream announced ${n:-0} of ${items} snapshot rows in two minutes" >&2; exit 1; }
  sleep 2
done
echo "menu stream: the snapshot of ${items} items announced (${n} records)"

say "Playing the night at ${SPEED}x, and killing Worker 1 at half-time"
python3 -I tools/feed_pings.py --speed "${SPEED}" --start-at "${start_at}" &
pings_pid=$!
python3 -I tools/feed_orders.py --speed "${SPEED}" --start-at "${start_at}" &
orders_pid=$!
python3 -I tools/feed_kitchen.py --speed "${SPEED}" --start-at "${start_at}" &
kitchen_pid=$!
python3 -I tools/feed_menus.py --speed "${SPEED}" --start-at "${start_at}" &
menus_pid=$!
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
if [ "${SCENES}" = 1 ]; then
  # Minute 75: the gauges job is stopped at a savepoint, which commits its
  # tail, and resubmitted from that savepoint as a new job.
  for _ in $(seq 1 1200); do
    m="$(cat "${STATE}/orders.progress" 2>/dev/null || echo -99)"
    [ "${m}" -ge 75 ] && break
    sleep 0.5
  done
  stopped="$("${BIN}/clink" stop --job-id="${gauges_job}" --coordinator-host=127.0.0.1 --coordinator-port="$(coord_ctl)")"
  echo "match minute ${m}: ${stopped}"
  savepoint="$(printf '%s' "${stopped}" | sed -nE 's/.*savepoint_checkpoint_id=([0-9]+).*/\1/p')"
  backend="$(curl -fsS "http://127.0.0.1:$(coord_http)/api/v1/jobs/${gauges_job}" | python3 -c 'import json, sys; print(json.load(sys.stdin).get("state_backend_uri", ""))')"
  "${BIN}/clink_submit_sql" --file "${STATE}/gauges.sql" --coordinator-host 127.0.0.1 --coordinator-port "$(coord_http)" \
    --name gauges-resumed --checkpoint-dir "${STATE}/checkpoints" --checkpoint-interval-ms 2000 \
    ${backend:+--state-backend "${backend}"} \
    --restore-from-dir "${STATE}/checkpoints/_jobs/${gauges_job}" --restore-from-checkpoint-id "${savepoint}"
  echo "the gauges job resumed from savepoint ${savepoint}"
  # Minute 95: the leading Coordinator is killed; the standby takes the leader
  # lock, binds its ports, and recovers every job from its checkpoints.
  for _ in $(seq 1 1200); do
    m="$(cat "${STATE}/orders.progress" 2>/dev/null || echo -99)"
    [ "${m}" -ge 95 ] && break
    sleep 0.5
  done
  echo "match minute ${m}: killing the leading Coordinator"
  operators_of "${kitchen_job}" "${STATE}/kitchen-operators-before-failover.json"
  kill -9 "${coord_pid}"
  wait "${coord_pid}" 2>/dev/null || true
  await_http "http://127.0.0.1:${CLINK_STANDBY_HTTP_PORT}/api/v1/health" "the standby Coordinator"
  echo "the standby Coordinator leads (http://localhost:${CLINK_STANDBY_HTTP_PORT})"
fi
wait "${pings_pid}"
wait "${orders_pid}"
wait "${kitchen_pid}"
wait "${menus_pid}"

say "Producers resend a batch of reports and a batch of tablet events they had already delivered"
python3 -I tools/feed_pings.py --resend 2000
python3 -I tools/feed_kitchen.py --resend 500

say "Reading the dispatch archive back into the analytics store"
# The dispatcher's tail: the orders still in their window when the quotes
# stopped are decided once the idle timeout releases them, and the archive
# commits them at its next checkpoint. The backfill reads what the archive
# holds when it starts, so it waits for the topic to stop growing first.
last=-1
for _ in $(seq 1 12); do
  n="$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 \
         --topic dispatch.decisions 2>/dev/null | awk -F: '{s += $NF} END {print s + 0}' || echo 0)"
  [ "${n}" = "${last}" ] && [ "${n}" != "-1" ] && break
  last="${n}"
  sleep 20
done
sleep 5
"${BIN}/clink" run "${STATE}/backfill.sql" --coordinator-host=127.0.0.1 \
  --coordinator-port="$(coord_http)" --name=backfill --parallelism=1 \
  --checkpoint-dir="${STATE}/checkpoints" --checkpoint-interval-ms=2000

[ -n "${kitchen_job:-}" ] && operators_of "${kitchen_job}" "${STATE}/kitchen-operators-at-the-end.json"

say "Checking every store against the reference model"
rc=0
python3 -I tools/check_run.py --wait 300 || rc=1
python3 -I tools/check_round2.py --wait 120 || rc=1
python3 -I tools/check_round3.py --wait 120 || rc=1
if [ "${SCENES}" = 1 ]; then
  n="$(ls "${STATE}/traces" | wc -l | tr -d ' ')"
  lines="$(cat "${STATE}/traces"/*.ndjson | wc -l | tr -d ' ')"
  if [ "${TRACE_CHECK:-0}" = 1 ] && [ -x "${CLINK_SRC}/scripts/formal-check.sh" ]; then
    say "Checking the run's protocol trace against the exactly-once specification"
    if "${CLINK_SRC}/scripts/formal-check.sh" --trace "${STATE}/traces" >"${STATE}/trace-check.log" 2>&1; then
      echo "protocol trace: ${lines} events in ${n} process files, accepted by the specification  OK"
    else
      echo "protocol trace: not accepted (see ${STATE}/trace-check.log)  FAIL"
      tail -20 "${STATE}/trace-check.log"
      rc=1
    fi
  else
    # The specification's trace validator reads one job at a time, and the
    # sink-side events carry no job id yet, so a trace of seventeen jobs is
    # not separable by job. Recorded here for when it is; TRACE_CHECK=1 runs
    # the validator regardless, with CLINK_SRC pointing at a clink checkout.
    echo "protocol trace: ${lines} events in ${n} process files recorded under ${STATE}/traces (not checked: see README)"
  fi
fi
say "What the run cost"
kill "${footprint_pid}" 2>/dev/null || true
python3 -I tools/footprint.py --summary "${STATE}/footprint.jsonl"
echo
echo "Neo4j Browser: http://localhost:${NEO4J_HTTP_PORT} (neo4j / ${NEO4J_PASSWORD})."
echo "RabbitMQ management: http://localhost:${RABBITMQ_HTTP_PORT} (halftime / halftime)."
echo "Grafana: http://localhost:${GRAFANA_HOST_PORT} (admin / halftime, the half-time dashboard). Prometheus: http://localhost:${PROMETHEUS_HOST_PORT}."
echo "Coordinator: http://localhost:$(coord_http). Logs: ${STATE}."
exit "${rc}"
