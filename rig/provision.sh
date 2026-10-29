#!/usr/bin/env bash
# Provision a half-time rig on Hetzner Cloud, every resource labelled per run
# so rig/teardown.sh can remove it all by label.
#
#   RUN_ID=<run id> ./rig/provision.sh --dry                       print the plan and the price; touch nothing
#   RUN_ID=<run id> HT_HCLOUD_CONTEXT=<context> ./rig/provision.sh  create the rig and write its inventory
#   WORKERS=12 WORKER_TYPE=ccx33 RUN_ID=... ./rig/provision.sh      twelve dedicated-core workers
#
# Topology, one host per role group so each system has a machine of its
# own and the Workers scale by count:
#
#   ht-<id>-ops            feeders, the match feed, the checks, Prometheus, Grafana, the Pushgateway;
#                          exports /ht/state over NFS (checkpoints, the HA directory)
#   ht-<id>-coordinator1   the leading clink Coordinator
#   ht-<id>-coordinator2   the standby
#   ht-<id>-worker{1..N}   clink Workers (N = WORKERS, default 3; bounded by the project's quota only)
#   ht-<id>-kafka          the broker
#   ht-<id>-stores         PostgreSQL, Neo4j, ClickHouse, MySQL
#   ht-<id>-messaging      Pulsar, RabbitMQ, NATS, Redis, Cassandra
#   ht-<id>-search         OpenSearch, InfluxDB
#
# The stores can be split further (one host each) when a run outgrows a
# host; the role compose files under rig/roles are per system group, so a
# split is a matter of which hosts run which file.
#
# Nothing here has been run against a real project yet: the dry run is the
# deliverable of the round that wrote it, and the first real run is a
# decision with a bill.
set -euo pipefail

DRY=0
[ "${1:-}" = "--dry" ] && DRY=1

RUN_ID="${RUN_ID:?set RUN_ID to a run id (e.g. final-20261101)}"
LABELS="ht=1,ht-run=${RUN_ID}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NET_NAME="ht-${RUN_ID}-net"
NET_RANGE="10.30.0.0/16"
SUBNET="10.30.1.0/24"
SSH_KEY_NAME="ht-key"
SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/half-time-ed25519}"

OPS_TYPE="${OPS_TYPE:-cpx32}"
COORD_TYPE="${COORD_TYPE:-cpx22}"
WORKER_TYPE="${WORKER_TYPE:-cpx32}"
KAFKA_TYPE="${KAFKA_TYPE:-cpx32}"
STORES_TYPE="${STORES_TYPE:-cpx42}"
MESSAGING_TYPE="${MESSAGING_TYPE:-cpx42}"
SEARCH_TYPE="${SEARCH_TYPE:-cpx32}"
WORKERS="${WORKERS:-3}"

echo "run id  : ${RUN_ID}"
echo "plan    : 1x ${OPS_TYPE} (ops) + 2x ${COORD_TYPE} (coordinators) + ${WORKERS}x ${WORKER_TYPE} (workers)"
echo "          + 1x ${KAFKA_TYPE} (kafka) + 1x ${STORES_TYPE} (stores) + 1x ${MESSAGING_TYPE} (messaging) + 1x ${SEARCH_TYPE} (search)"
echo "location: ${LOCATION}; labels ${LABELS}"

# The price, from Hetzner's own list, when an hcloud context is at hand; a
# dry run without one still prints the plan.
if command -v hcloud >/dev/null 2>&1 && [ -n "$(hcloud context active 2>/dev/null || true)" ]; then
  hcloud server-type list -o json | python3 -c "
import json, sys
types = json.load(sys.stdin)
want = {}
for name, count in (('${OPS_TYPE}', 1), ('${COORD_TYPE}', 2), ('${WORKER_TYPE}', ${WORKERS}), ('${KAFKA_TYPE}', 1),
                    ('${STORES_TYPE}', 1), ('${MESSAGING_TYPE}', 1), ('${SEARCH_TYPE}', 1)):
    want[name] = want.get(name, 0) + count
total = 0.0
for t in types:
    if t.get('name') not in want:
        continue
    for pr in t.get('prices') or []:
        if pr.get('location') != '${LOCATION}':
            continue
        total += float(pr.get('price_hourly', {}).get('gross', 0) or 0) * want[t['name']]
        break
print('cost    : EUR %.3f/hour (~EUR %.2f for a four-hour evening, ~EUR %.2f/day)' % (total, total * 4, total * 24))
"
else
  echo "cost    : (an active hcloud context is needed to read the price list)"
fi

if [ "$DRY" = "1" ]; then
  echo "--dry: nothing created."
  exit 0
fi

# The active hcloud context decides who is billed. Creating anything needs
# the context named explicitly, and it must be the active one.
command -v hcloud >/dev/null 2>&1 || { echo "provision: hcloud CLI not found" >&2; exit 2; }
HT_HCLOUD_CONTEXT="${HT_HCLOUD_CONTEXT:?set HT_HCLOUD_CONTEXT to the hcloud context this rig may bill}"
active="$(hcloud context active 2>/dev/null || true)"
if [ "${active}" != "${HT_HCLOUD_CONTEXT}" ]; then
  echo "REFUSING TO PROVISION: the active hcloud context is '${active:-<none>}', HT_HCLOUD_CONTEXT says" >&2
  echo "  '${HT_HCLOUD_CONTEXT}'. Switch with: hcloud context use ${HT_HCLOUD_CONTEXT}" >&2
  exit 1
fi

if [ ! -f "${SSH_KEY_FILE}" ]; then
  echo "==> generating ${SSH_KEY_FILE}"
  ssh-keygen -t ed25519 -N "" -f "${SSH_KEY_FILE}" -C "half-time" >/dev/null
fi
if ! hcloud ssh-key describe "${SSH_KEY_NAME}" >/dev/null 2>&1; then
  echo "==> uploading the ssh key"
  hcloud ssh-key create --name "${SSH_KEY_NAME}" --public-key-from-file "${SSH_KEY_FILE}.pub" --label "ht=1" >/dev/null
fi
if ! hcloud network describe "${NET_NAME}" >/dev/null 2>&1; then
  echo "==> creating network ${NET_NAME} (${NET_RANGE})"
  hcloud network create --name "${NET_NAME}" --ip-range "${NET_RANGE}" --label "${LABELS}" >/dev/null
  hcloud network add-subnet "${NET_NAME}" --network-zone eu-central --type cloud --ip-range "${SUBNET}" >/dev/null
fi

# Two cloud-inits: the ops host exports /ht/state over NFS, every clink host
# mounts it. Checkpoint state is addressed by subtask index on the node's
# own filesystem, so a Worker's subtask redeployed on another host after a
# kill must find the same directory there.
OPS_INIT="$(mktemp)"
cat >"${OPS_INIT}" <<'YAML'
#cloud-config
package_update: true
packages: [docker.io, docker-compose-v2, python3, rsync, nfs-kernel-server]
runcmd:
  - systemctl enable --now docker
  - sysctl -w vm.max_map_count=262144
  - mkdir -p /ht/state && chmod 777 /ht/state
  - echo "/ht/state 10.30.1.0/24(rw,sync,no_subtree_check,no_root_squash)" >> /etc/exports
  - exportfs -ra
  - systemctl enable --now nfs-kernel-server
YAML
NODE_INIT="$(mktemp)"
cat >"${NODE_INIT}" <<'YAML'
#cloud-config
package_update: true
packages: [docker.io, docker-compose-v2, nfs-common]
runcmd:
  - systemctl enable --now docker
  - sysctl -w vm.max_map_count=262144
  - mkdir -p /ht/state
YAML

create() {  # <name> <type> <cloud-init>
  if hcloud server describe "$1" >/dev/null 2>&1; then
    echo "==> $1 exists, skipping"
    return 0
  fi
  echo "==> creating $1 ($2)"
  hcloud server create --name "$1" --type "$2" --image "${IMAGE}" --location "${LOCATION}" \
    --ssh-key "${SSH_KEY_NAME}" --network "${NET_NAME}" --user-data-from-file "$3" --label "${LABELS}" >/dev/null
}
create "ht-${RUN_ID}-ops" "${OPS_TYPE}" "${OPS_INIT}"
create "ht-${RUN_ID}-coordinator1" "${COORD_TYPE}" "${NODE_INIT}"
create "ht-${RUN_ID}-coordinator2" "${COORD_TYPE}" "${NODE_INIT}"
for i in $(seq 1 "${WORKERS}"); do create "ht-${RUN_ID}-worker${i}" "${WORKER_TYPE}" "${NODE_INIT}"; done
create "ht-${RUN_ID}-kafka" "${KAFKA_TYPE}" "${NODE_INIT}"
create "ht-${RUN_ID}-stores" "${STORES_TYPE}" "${NODE_INIT}"
create "ht-${RUN_ID}-messaging" "${MESSAGING_TYPE}" "${NODE_INIT}"
create "ht-${RUN_ID}-search" "${SEARCH_TYPE}" "${NODE_INIT}"
rm -f "${OPS_INIT}" "${NODE_INIT}"

# The Coordinator's HTTP API takes job submissions, and a job is code, so
# nothing but ssh, the Coordinators' HTTP ports and the dashboards is open,
# and only to the operator's address; the rig talks to itself privately.
OPERATOR_IP="${OPERATOR_IP:-$(curl -fsS https://api.ipify.org 2>/dev/null || true)}"
[ -n "${OPERATOR_IP}" ] || { echo "provision: set OPERATOR_IP=<your public address>; refusing to leave the rig open" >&2; exit 2; }
FW="ht-${RUN_ID}-fw"
hcloud firewall describe "${FW}" >/dev/null 2>&1 || hcloud firewall create --name "${FW}" --label "ht=1" --label "ht-run=${RUN_ID}" >/dev/null
RULES="$(mktemp)"
cat >"${RULES}" <<JSON
[
  {"direction":"in","protocol":"tcp","port":"22","source_ips":["${OPERATOR_IP}/32"],"description":"ssh, operator only"},
  {"direction":"in","protocol":"tcp","port":"any","source_ips":["${NET_RANGE}"],"description":"rig-internal"},
  {"direction":"in","protocol":"udp","port":"any","source_ips":["${NET_RANGE}"],"description":"rig-internal"},
  {"direction":"in","protocol":"tcp","port":"8081","source_ips":["${OPERATOR_IP}/32"],"description":"coordinator HTTP, operator only"},
  {"direction":"in","protocol":"tcp","port":"3000","source_ips":["${OPERATOR_IP}/32"],"description":"grafana, operator only"},
  {"direction":"in","protocol":"tcp","port":"9090","source_ips":["${OPERATOR_IP}/32"],"description":"prometheus, operator only"}
]
JSON
hcloud firewall replace-rules "${FW}" --rules-file "${RULES}" >/dev/null
rm -f "${RULES}"
for s in $(hcloud server list -l "ht-run=${RUN_ID}" -o noheader -o columns=name); do
  hcloud firewall apply-to-resource "${FW}" --type server --server "${s}" >/dev/null 2>&1 || true
done
echo "==> firewall applied to every host (operator ${OPERATOR_IP})"

"$(dirname "$0")/inventory.sh"
echo
echo "REMINDER: these bill until destroyed:  ./rig/teardown.sh ${RUN_ID}   then   ./rig/teardown.sh --check"
