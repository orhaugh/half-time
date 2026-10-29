#!/usr/bin/env bash
# Run half-time on a provisioned rig, from this machine, over ssh: the systems
# on their hosts, clink from the runtime image, the night played from the ops
# host and every store checked there.
#
#   RUN_ID=<run id> ./rig/run.sh --dry      print every step and command; connect to nothing
#   RUN_ID=<run id> ./rig/run.sh            the run, against rig/inventory/<run id>.json
#
# The scale is the environment's:
#   CELLS=12 RIDERS_PER_CELL=300 RESTAURANTS_PER_CELL=40 CUSTOMERS_PER_CELL=4000 ORDERS_PER_MINUTE=60
#                                        the night's size (tools/world.py's flags)
#   SPEED=30                             how fast it plays
#   SLOTS=32                             per Worker; PARALLELISM (default Workers x SLOTS / 4) for the keyed jobs
#   FEEDERS=4                            feeder processes per stream, each a share of the cities
#   PARTITIONS=12                        per Kafka topic
#   CLINK_IMAGE=ghcr.io/orhaugh/clink-runtime:<tag>
#
# Nothing here has been run against a real rig yet: the dry run is what this
# round delivers, and the first real run is a decision with a bill.
set -euo pipefail
cd "$(dirname "$0")/.."
DRY=0
[ "${1:-}" = "--dry" ] && DRY=1
RUN_ID="${RUN_ID:?set RUN_ID to the run id}"
INV="rig/inventory/${RUN_ID}.json"
KEY="${SSH_KEY_FILE:-$HOME/.ssh/half-time-ed25519}"
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=yes -i "${KEY}")
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"
CELLS="${CELLS:-12}"; RIDERS_PER_CELL="${RIDERS_PER_CELL:-300}"; RESTAURANTS_PER_CELL="${RESTAURANTS_PER_CELL:-40}"
CUSTOMERS_PER_CELL="${CUSTOMERS_PER_CELL:-4000}"; ORDERS_PER_MINUTE="${ORDERS_PER_MINUTE:-60}"
SPEED="${SPEED:-30}"; SLOTS="${SLOTS:-32}"; FEEDERS="${FEEDERS:-4}"; PARTITIONS="${PARTITIONS:-12}"
WORKERS="${WORKERS:-3}"

# The inventory: private addresses by role. A dry run without one uses the
# role names, so the plan reads.
if [ -f "${INV}" ]; then
  inv() { python3 -c "
import json, sys
hosts = [h for h in json.load(open('${INV}'))['hosts'] if h['role'] == sys.argv[1]]
print(' '.join(h[sys.argv[2]] for h in hosts))" "$1" "$2"; }
  WORKERS="$(inv worker private_ip | wc -w | tr -d ' ')"
elif [ "${DRY}" = 1 ]; then
  inv() {
    case "$1" in
      worker) seq -f "<worker%g>" 1 "${WORKERS}" | tr '\n' ' ' ;;
      coordinator) echo "<coordinator1> <coordinator2>" ;;
      *) echo "<$1>" ;;
    esac
  }
else
  echo "rig/run.sh: no inventory at ${INV}; run rig/provision.sh (or rig/inventory.sh) first" >&2; exit 2
fi
OPS="$(inv ops private_ip)"; OPS_PUB="$(inv ops public_ip)"
COORD1="$(inv coordinator private_ip | cut -d' ' -f1)"; COORD1_PUB="$(inv coordinator public_ip | cut -d' ' -f1)"
COORD2="$(inv coordinator private_ip | cut -d' ' -f2)"; COORD2_PUB="$(inv coordinator public_ip | cut -d' ' -f2)"
KAFKA="$(inv kafka private_ip)"; STORES="$(inv stores private_ip)"; MSG="$(inv messaging private_ip)"; SEARCH="$(inv search private_ip)"
PARALLELISM="${PARALLELISM:-$(( WORKERS * SLOTS / 4 ))}"
[ "${PARALLELISM}" -ge 1 ] || PARALLELISM=1

say() { printf '\n==> %s\n' "$*"; }
on_host() {  # <public ip> <command>
  if [ "${DRY}" = 1 ]; then printf '  [%s] %s\n' "$1" "$2"; return 0; fi
  ssh -n "${SSH_OPTS[@]}" "root@$1" "$2"
}
to_host() {  # <public ip> <local path> <remote path>
  if [ "${DRY}" = 1 ]; then printf '  [%s] <- %s -> %s\n' "$1" "$2" "$3"; return 0; fi
  rsync -az --delete --exclude state --exclude .git -e "ssh ${SSH_OPTS[*]}" "$2" "root@$1:$3"
}
pub_of() {  # <role> <index from 1>: the public address of the nth host of a role
  inv "$1" public_ip | cut -d' ' -f"$2"
}

# Every tool on the ops host reaches the systems through these.
HT="HT_MODE=rig HT_KAFKA=${KAFKA}:9092 HT_POSTGRES=${STORES}:5432 HT_CLICKHOUSE=${STORES}:9000 HT_NEO4J=http://${STORES}:7474"
HT="${HT} HT_MYSQL=${STORES}:3306 HT_PULSAR=${MSG} HT_RABBITMQ_HTTP=http://${MSG}:15672 HT_NATS=${MSG}:4222 HT_REDIS=${MSG}:6379"
HT="${HT} HT_CASSANDRA=${MSG}:9042 HT_OPENSEARCH=http://${SEARCH}:9200 HT_INFLUXDB=http://${SEARCH}:8086 HT_PUSHGATEWAY=http://${OPS}:9091"
HT="${HT} NEO4J_PASSWORD=half-time-pw"
OPS_CMD() { on_host "${OPS_PUB}" "cd /ht/half-time && export ${HT} && $1"; }

echo "rig ${RUN_ID}: ${WORKERS} Workers x ${SLOTS} slots, keyed jobs at parallelism ${PARALLELISM}, ${FEEDERS} feeders per stream,"
echo "  ${CELLS} cities x ${RIDERS_PER_CELL} riders, ${PARTITIONS} partitions per topic, clink ${CLINK_IMAGE}"
[ "${DRY}" = 1 ] && echo "  --dry: every command is printed, none is run"

say "The repository, to every host"
for h in "${OPS_PUB}" "${COORD1_PUB}" "${COORD2_PUB}" $(inv worker public_ip) $(pub_of kafka 1) $(pub_of stores 1) $(pub_of messaging 1) $(pub_of search 1); do
  on_host "${h}" "mkdir -p /ht/half-time"
  to_host "${h}" ./ /ht/half-time/
done

say "The shared state: the ops host exports it, every other host mounts it"
on_host "${OPS_PUB}" "mkdir -p /ht/state/checkpoints /ht/state/archive /ht/state/ha /ht/state/traces && chmod -R 777 /ht/state && ln -sfn /ht/state/archive /ht/half-time/state/archive"
for h in "${COORD1_PUB}" "${COORD2_PUB}" $(inv worker public_ip) $(pub_of stores 1); do
  on_host "${h}" "mkdir -p /ht/state && (mountpoint -q /ht/state || mount -t nfs4 ${OPS}:/ht/state /ht/state)"
done

say "The systems, each group on its host"
on_host "$(pub_of kafka 1)" "cd /ht/half-time && PRIVATE_IP=${KAFKA} PARTITIONS=${PARTITIONS} docker compose -f rig/roles/kafka.yml up -d --wait kafka && PRIVATE_IP=${KAFKA} PARTITIONS=${PARTITIONS} docker compose -f rig/roles/kafka.yml run --rm --no-deps -T kafka-init"
on_host "$(pub_of stores 1)" "cd /ht/half-time && PRIVATE_IP=${STORES} docker compose -f rig/roles/stores.yml up -d --wait"
on_host "$(pub_of messaging 1)" "cd /ht/half-time && PRIVATE_IP=${MSG} docker compose -f rig/roles/messaging.yml up -d --wait pulsar rabbitmq nats redis cassandra && PRIVATE_IP=${MSG} docker compose -f rig/roles/messaging.yml run --rm --no-deps -T nats-init"
on_host "$(pub_of search 1)" "cd /ht/half-time && PRIVATE_IP=${SEARCH} docker compose -f rig/roles/search.yml up -d --wait"
# Prometheus's targets are the rig's addresses.
targets="\"${COORD1}:8081\", \"${COORD2}:8081\""
for w in $(inv worker private_ip); do targets="${targets}, \"${w}:8082\""; done
on_host "${OPS_PUB}" "mkdir -p /ht/half-time/rig/generated && printf '%s\n' 'global:' '  scrape_interval: 5s' 'scrape_configs:' '  - job_name: clink' '    static_configs:' '      - targets: [${targets}]' '  - job_name: pushgateway' '    honor_labels: true' '    static_configs:' '      - targets: [\"127.0.0.1:9091\"]' > /ht/half-time/rig/generated/prometheus.yml && cd /ht/half-time && docker compose -f rig/roles/ops.yml up -d"

say "The night, generated on the ops host, and the schemas and first rows"
OPS_CMD "python3 -I tools/world.py --out state/world --cells ${CELLS} --riders-per-cell ${RIDERS_PER_CELL} --restaurants-per-cell ${RESTAURANTS_PER_CELL} --customers-per-cell ${CUSTOMERS_PER_CELL} --orders-per-minute ${ORDERS_PER_MINUTE}"
OPS_CMD "docker run --rm -i --network host -e PGPASSWORD=halftime postgres:16.15 psql -q -v ON_ERROR_STOP=1 -h ${STORES} -U halftime -d halftime -f - < postgres/schema.sql"
OPS_CMD "docker run --rm -i --network host clickhouse/clickhouse-server:25.3 clickhouse-client --host ${STORES} --user halftime --password halftime --multiquery < clickhouse/schema.sql"
OPS_CMD "docker run --rm -i --network host -e MYSQL_PWD=halftime mysql:8.0 mysql -h ${STORES} -u root halftime < mysql/schema.sql"
OPS_CMD "docker run --rm -i --network host cassandra:4 cqlsh ${MSG} 9042 < cassandra/schema.cql"
OPS_CMD "python3 -I tools/feed_menus.py --initial && python3 -I tools/init_messaging.py && python3 -I tools/load_graph.py --reset"

say "clink: two Coordinators and ${WORKERS} Workers from ${CLINK_IMAGE}"
on_host "${COORD1_PUB}" "cd /ht/half-time && PRIVATE_IP=${COORD1} CLINK_IMAGE=${CLINK_IMAGE} docker compose -f rig/roles/coordinator.yml up -d"
OPS_CMD "for _ in \$(seq 1 60); do curl -fsS http://${COORD1}:8081/api/v1/health >/dev/null && break; sleep 2; done"
on_host "${COORD2_PUB}" "cd /ht/half-time && PRIVATE_IP=${COORD2} CLINK_IMAGE=${CLINK_IMAGE} docker compose -f rig/roles/coordinator.yml up -d"
i=0
for w in $(inv worker private_ip); do
  i=$((i + 1))
  on_host "$(pub_of worker "${i}")" "cd /ht/half-time && PRIVATE_IP=${w} WORKER_ID=worker-${i} SLOTS=${SLOTS} CLINK_IMAGE=${CLINK_IMAGE} docker compose -f rig/roles/worker.yml up -d"
done
OPS_CMD "for _ in \$(seq 1 60); do [ \"\$(curl -fsS http://${COORD1}:8081/api/v1/workers | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d[\"workers\"] if isinstance(d, dict) else d))')\" -ge ${WORKERS} ] && break; sleep 2; done"

say "The jobs: the same SQL, the rig's addresses"
OPS_CMD "mkdir -p state/rig-sql && for f in pipeline/*.sql; do sed -e 's#kafka:9092#${KAFKA}:9092#g' -e 's#http://neo4j:7474#http://${STORES}:7474#g' -e 's#host=postgres port=5432#host=${STORES} port=5432#g' -e \"s#host *= *'clickhouse'#host = '${STORES}'#g\" -e \"s#host *= *'mysql'#host = '${STORES}'#g\" -e 's#pulsar://pulsar:6650#pulsar://${MSG}:6650#g' -e \"s#host *= *'rabbitmq'#host = '${MSG}'#g\" -e 's#nats://nats:4222#nats://${MSG}:4222#g' -e \"s#host *= *'redis'#host = '${MSG}'#g\" -e \"s#contact_points *= *'cassandra'#contact_points = '${MSG}'#g\" -e 's#http://opensearch:9200#http://${SEARCH}:9200#g' -e 's#http://influxdb:8086#http://${SEARCH}:8086#g' -e 's#http://pushgateway:9091#http://${OPS}:9091#g' -e 's#http://match-feed:28090#http://${OPS}:28090#g' -e \"s#'/archive/#'/ht/state/archive/#g\" \"\$f\" > state/rig-sql/\$(basename \"\$f\"); done"
# The match feed first: the match job polls it from its first second.
OPS_CMD "mkdir -p state/run && start_at=\$(python3 -c 'import time; print(time.time() + 120)') && echo \$start_at > state/run/start_at && nohup python3 -I tools/match_feed.py --port 28090 --speed ${SPEED} --start-at \$start_at > state/run/match_feed.log 2>&1 &"
for job in orders pricing quotes dispatch ledgers analytics archive menus bridge kitchen ops tracker search match lake telemetry gauges; do
  p="${PARALLELISM}"
  case "${job}" in orders|menus|match|bridge|kitchen|gauges) p=1 ;; esac
  OPS_CMD "docker run --rm --network host -v /ht:/ht ${CLINK_IMAGE} run /ht/half-time/state/rig-sql/${job}.sql --coordinator-host ${COORD1} --coordinator-port 8081 --name ${job} --parallelism ${p} --checkpoint-dir /ht/state/checkpoints --checkpoint-interval-ms 2000"
done

say "The night at ${SPEED}x from the ops host, ${FEEDERS} feeders per stream, and a Worker killed at half-time"
OPS_CMD "start_at=\$(cat state/run/start_at); for i in \$(seq 0 $((FEEDERS - 1))); do nohup python3 -I tools/feed_pings.py --speed ${SPEED} --start-at \$start_at --shard \$i/${FEEDERS} > state/run/feed_pings-\$i.log 2>&1 & nohup python3 -I tools/feed_orders.py --speed ${SPEED} --start-at \$start_at --shard \$i/${FEEDERS} > state/run/feed_orders-\$i.log 2>&1 & nohup python3 -I tools/feed_kitchen.py --speed ${SPEED} --start-at \$start_at --shard \$i/${FEEDERS} > state/run/feed_kitchen-\$i.log 2>&1 & done; nohup python3 -I tools/feed_menus.py --speed ${SPEED} --start-at \$start_at > state/run/feed_menus.log 2>&1 &"
OPS_CMD "for _ in \$(seq 1 3000); do m=\$(cat state/run/orders-0.progress 2>/dev/null || echo -99); [ \"\$m\" -ge 50 ] && break; sleep 1; done; echo match minute \$m"
on_host "$(pub_of worker 1)" "docker kill clink-worker; sleep 5; cd /ht/half-time && PRIVATE_IP=$(inv worker private_ip | cut -d' ' -f1) WORKER_ID=worker-1 SLOTS=${SLOTS} CLINK_IMAGE=${CLINK_IMAGE} docker compose -f rig/roles/worker.yml up -d"
OPS_CMD "while pgrep -f 'tools/feed_' >/dev/null; do sleep 5; done; echo the night is over"

say "Resends, the backfill, and the checks"
OPS_CMD "python3 -I tools/feed_pings.py --resend 2000 && python3 -I tools/feed_kitchen.py --resend 500"
OPS_CMD "docker run --rm --network host -v /ht:/ht ${CLINK_IMAGE} run /ht/half-time/state/rig-sql/backfill.sql --coordinator-host ${COORD1} --coordinator-port 8081 --name backfill --parallelism 1 --checkpoint-dir /ht/state/checkpoints --checkpoint-interval-ms 2000"
OPS_CMD "python3 -I tools/check_run.py --scale --wait 900; python3 -I tools/check_round2.py --wait 600; python3 -I tools/check_round3.py --wait 600"
echo
echo "Grafana: http://${OPS_PUB}:3000 (admin / halftime). Coordinator: http://${COORD1_PUB}:8081."
echo "When done: ./rig/teardown.sh ${RUN_ID} && ./rig/teardown.sh --check"
