#!/usr/bin/env bash
# Run half-time end to end, a Worker kill included, and check every store it
# writes against what the night's data says.
#
#   CLINK_BIN_DIR=<clink build dir> ./run.sh   clink from a local build (clink and clink_node)
#   SPEED=30 ./run.sh                          the night at thirty times real time
#   DISPATCH_PARALLELISM=3 ./run.sh            the dispatcher three ways (see README)
#   SCENES=0 ./run.sh                          the night without the operational scenes
#   CLINK_SRC=<clink checkout> ./run.sh        where scripts/formal-check.sh is (default ../clink)
#   CAPTURE_JOBS="kitchen" ./run.sh            record these jobs with clink's flight recorder (see README)
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
# from its checkpoints. Around them, the dispatcher's join is scaled to twice
# its parallelism for the surge after the minute-67 goal, kept there through
# the failover, and scaled back after the last goal. Every clink process records its protocol trace under
# state/run/traces; TRACE_CHECK=1 hands it to the exactly-once
# specification's validator after the stores are checked, one job at a time
# (ten to twenty-five minutes, and it needs Java and a clink checkout).
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
# The jobs to record with clink's flight recorder, by name: each of their
# operators writes the input it was handed, per checkpoint epoch, under
# state/run/capture/<job>, which `clink capture-cat` inspects and `clink
# replay` reproduces offline, from the checkpoint before the epoch (see the
# README, "Dissecting it").
CAPTURE_JOBS="${CAPTURE_JOBS:-}"
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
# Every process the run started: clink's, the match feed and the footprint
# sampler. A process that has not stopped ten seconds after it is asked to is
# killed.
stop_processes() {
  local pid
  for pid in ${pids[@]+"${pids[@]}"} ${worker1_pid}; do
    kill "${pid}" 2>/dev/null || true
  done
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
}
cleanup() {
  stop_processes
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
docker compose exec -T clickhouse clickhouse-client --multiquery <clickhouse/schema.sql
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
  # The same SQL, reaching the services through their host ports, which listen
  # on the IPv4 loopback only (see docker-compose.yml): hence 127.0.0.1, never
  # localhost, which a client may try as ::1 first.
  sed -e "s#kafka:9092#127.0.0.1:${KAFKA_HOST_PORT}#g" \
      -e "s#http://neo4j:7474#http://127.0.0.1:${NEO4J_HTTP_PORT}#g" \
      -e "s#host=postgres port=5432#host=127.0.0.1 port=${POSTGRES_HOST_PORT}#g" \
      -e "s#host *= *'clickhouse'#host = '127.0.0.1'#g" \
      -e "s#port *= *'9000'#port = '${CLICKHOUSE_HOST_PORT}'#g" \
      -e "s#'/archive/#'${ROOT}/state/archive/#g" \
      -e "s#host *= *'mysql'#host = '127.0.0.1'#g" \
      -e "s#port *= *'3306'#port = '${MYSQL_HOST_PORT}'#g" \
      -e "s#pulsar://pulsar:6650#pulsar://127.0.0.1:6650#g" \
      -e "s#host *= *'rabbitmq'#host = '127.0.0.1'#g" \
      -e "s#port *= *'5672'#port = '${RABBITMQ_HOST_PORT}'#g" \
      -e "s#nats://nats:4222#nats://127.0.0.1:${NATS_HOST_PORT}#g" \
      -e "s#host *= *'redis'#host = '127.0.0.1'#g" \
      -e "s#port *= *'6379'#port = '${REDIS_HOST_PORT}'#g" \
      -e "s#contact_points *= *'cassandra'#contact_points = '127.0.0.1'#g" \
      -e "s#http://opensearch:9200#http://127.0.0.1:${OPENSEARCH_HOST_PORT}#g" \
      -e "s#http://influxdb:8086#http://127.0.0.1:${INFLUXDB_HOST_PORT}#g" \
      -e "s#http://pushgateway:9091#http://127.0.0.1:${PUSHGATEWAY_HOST_PORT}#g" \
      -e "s#http://match-feed:28090#http://127.0.0.1:${MATCH_FEED_PORT}#g" \
      "${f}" >"${STATE}/${job}.sql"
done
captured() {  # <job>: whether CAPTURE_JOBS names it
  case " ${CAPTURE_JOBS} " in *" $1 "*) return 0 ;; esac
  return 1
}
# Each job's id, as the Coordinator assigned it: state/run/jobs.tsv.
: >"${STATE}/jobs.tsv"
for job in orders pricing quotes dispatch ledgers analytics archive menus bridge kitchen ops tracker \
           search match lake telemetry gauges; do
  p=1; [ "${job}" = dispatch ] && p="${DISPATCH_PARALLELISM}"
  capture=""; captured "${job}" && capture="--capture-dir=${STATE}/capture/${job}"
  # The dispatcher declares its rescalable stages, up to twice its
  # parallelism, and names them on stderr: the join is resized below.
  rescalable=""; [ "${job}" = dispatch ] && [ "${SCENES}" = 1 ] && rescalable="--max-parallelism=$((2 * p))"
  submitted="$("${BIN}/clink" run "${STATE}/${job}.sql" --coordinator-host=127.0.0.1 \
    --coordinator-port="${CLINK_HTTP_PORT}" --name="${job}" --parallelism="${p}" \
    --checkpoint-dir="${STATE}/checkpoints" --checkpoint-interval-ms=2000 ${capture:+"${capture}"} \
    ${rescalable:+"${rescalable}"} 2>"${STATE}/${job}.submit.err")" \
    || { cat "${STATE}/${job}.submit.err" >&2; exit 1; }
  echo "${submitted}"
  grep -h '^rescalable' "${STATE}/${job}.submit.err" || true
  id="$(python3 -c 'import json, sys; print(json.loads(sys.argv[1])["job_id"])' "${submitted}")"
  printf '%s\t%s\n' "${job}" "${id}" >>"${STATE}/jobs.tsv"
  [ "${job}" = gauges ] && gauges_job="${id}"
  [ "${job}" = kitchen ] && kitchen_job="${id}"
  [ "${job}" = dispatch ] && dispatch_job="${id}"
done
# The dispatcher's join, as its submission named it among the rescalable stages.
dispatch_join="$(sed -nE 's/^rescalable up to parallelism [0-9]+: //p' "${STATE}/dispatch.submit.err" \
  | tr ',' '\n' | tr -d ' ' | grep '^ijoin_' | head -1 || true)"
# The parallelism the dispatcher's graph records for an operator.
graph_parallelism() {  # <job id> <operator id>
  curl -fsS "http://127.0.0.1:$(coord_http)/api/v1/jobs/$1/graph" 2>/dev/null | python3 -c '
import json, sys
nodes = json.load(sys.stdin).get("nodes", [])
print(next((n.get("parallelism", 1) for n in nodes if n.get("id") == sys.argv[1]), "?"))' "$2" \
    || echo "?"
}
rescale_join() {  # <parallelism>
  "${BIN}/clink" rescale-op --job-id="${dispatch_job}" --op="${dispatch_join}" --parallelism="$1" \
    --coordinator-host=127.0.0.1 --coordinator-port="$(coord_ctl)"
}
await_minute() {  # <match minute>
  for _ in $(seq 1 1200); do
    m="$(cat "${STATE}/orders.progress" 2>/dev/null || echo -99)"
    [ "${m}" -ge "$1" ] && return 0
    sleep 0.5
  done
}
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
  # Minute 67, a goal: orders surge for two minutes, and the dispatcher's
  # join, which pairs each order with the riders near it, is scaled to twice
  # its parallelism while it runs. It restores its state from the last
  # completed checkpoint at the new layout, and the job replays from there.
  await_minute 67
  echo "match minute ${m}: the dispatcher's join ${dispatch_join:-(none named)} to $((2 * DISPATCH_PARALLELISM)) subtasks: $(rescale_join $((2 * DISPATCH_PARALLELISM)) 2>&1)"
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
  capture=""; captured gauges && capture="${STATE}/capture/gauges"
  "${BIN}/clink_submit_sql" --file "${STATE}/gauges.sql" --coordinator-host 127.0.0.1 --coordinator-port "$(coord_http)" \
    --name gauges-resumed --checkpoint-dir "${STATE}/checkpoints" --checkpoint-interval-ms 2000 \
    ${backend:+--state-backend "${backend}"} ${capture:+--capture-dir "${capture}"} \
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
  # A recorded job's newest snapshots, copied before the kill: the takeover
  # restores from the newest completed checkpoint, which the checkpoint
  # directory keeps only until a newer one completes, and a replay of the
  # epochs after the restore starts from that state.
  for job in ${CAPTURE_JOBS}; do
    id="$(awk -F '\t' -v n="${job}" '$1 == n { print $2 }' "${STATE}/jobs.tsv")"
    [ -n "${id}" ] && [ -d "${STATE}/checkpoints/_state/${id}" ] \
      && cp -R "${STATE}/checkpoints/_state/${id}" "${STATE}/capture/${job}/state-before-failover"
  done
  kill -9 "${coord_pid}"
  wait "${coord_pid}" 2>/dev/null || true
  await_http "http://127.0.0.1:${CLINK_STANDBY_HTTP_PORT}/api/v1/health" "the standby Coordinator"
  echo "the standby Coordinator leads (http://localhost:${CLINK_STANDBY_HTTP_PORT})"
  # The takeover planned the graph that wrote the checkpoint it restored: the
  # rescaled one, which the manifest recorded when the join was resized.
  for _ in $(seq 1 60); do
    [ "$(graph_parallelism "${dispatch_job}" "${dispatch_join}")" != "?" ] && break
    sleep 1
  done
  echo "after the takeover the dispatcher's join runs as $(graph_parallelism "${dispatch_job}" "${dispatch_join}") subtasks"
  # Minute 110, after the last goal's surge: back to its parallelism.
  await_minute 110
  echo "match minute ${m}: the dispatcher's join back to ${DISPATCH_PARALLELISM}: $(rescale_join "${DISPATCH_PARALLELISM}" 2>&1)"
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
# holds when it starts, so it waits for the topic to stop growing first, and
# then for the archive's committed files to stop changing, which they do once
# the checkpoint that commits the last decisions completes.
last=-1
for _ in $(seq 1 12); do
  n="$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 \
         --topic dispatch.decisions 2>/dev/null | awk -F: '{s += $NF} END {print s + 0}' || echo 0)"
  [ "${n}" = "${last}" ] && [ "${n}" != "-1" ] && break
  last="${n}"
  sleep 20
done
last=""
for _ in $(seq 1 24); do
  n="$(ls -l "${ROOT}/state/archive/dispatch.decisions/committed" 2>/dev/null | awk '{print $5, $9}' | cksum)"
  [ "${n}" = "${last}" ] && break
  last="${n}"
  sleep 5
done
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
  check_trace=0
  [ "${TRACE_CHECK:-0}" = 1 ] && [ -x "${CLINK_SRC}/scripts/formal-check.sh" ] && check_trace=1
  # The trace is checked as the run left it. A running Coordinator or Worker
  # keeps appending to the files the check reads, so a ten-minute check read
  # a run ten minutes longer, job after job: one machine under pressure made
  # it ninety, and a job's trace outgrew the longest behaviour the checker
  # follows. Every process is stopped first, the footprint sampler with them,
  # so what the run cost leaves the check out.
  [ "${check_trace}" = 1 ] && stop_processes
  n="$(ls "${STATE}/traces" | wc -l | tr -d ' ')"
  lines="$(cat "${STATE}/traces"/*.ndjson | wc -l | tr -d ' ')"
  if [ "${check_trace}" = 1 ]; then
    say "Checking the run's protocol trace against the exactly-once specification"
    if TRACE_JOBS="${TRACE_JOBS:-4}" "${CLINK_SRC}/scripts/formal-check.sh" --trace "${STATE}/traces" \
        >"${STATE}/trace-check.log" 2>&1; then
      verdict="$(sed -nE 's/^formal-check: all ([0-9]+) validated trace\(s\) accepted; ([0-9]+) skipped.*/\1 jobs accepted, \2 outside the specification/p' "${STATE}/trace-check.log")"
      echo "protocol trace: ${lines} events in ${n} process files; ${verdict} (see ${STATE}/trace-check.log)  OK"
    else
      echo "protocol trace: not accepted (see ${STATE}/trace-check.log)  FAIL"
      tail -20 "${STATE}/trace-check.log"
      rc=1
    fi
  else
    # Validating every job's run against the specification takes ten to
    # twenty-five minutes and a Java runtime, so it is asked for:
    # TRACE_CHECK=1, with CLINK_SRC pointing at a clink checkout.
    echo "protocol trace: ${lines} events in ${n} process files recorded under ${STATE}/traces (not checked: TRACE_CHECK=1 checks it)"
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
