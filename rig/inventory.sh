#!/usr/bin/env bash
# The rig's inventory from Hetzner's own state, into rig/inventory/<RUN_ID>.json,
# the file rig/run.sh keys on. Reads cloud state only; never starts anything.
#
#   RUN_ID=<run id> ./rig/inventory.sh
set -euo pipefail
RUN_ID="${RUN_ID:?set RUN_ID to the run id}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${HERE}/inventory/${RUN_ID}.json"
mkdir -p "${HERE}/inventory"
command -v hcloud >/dev/null 2>&1 || { echo "inventory: hcloud CLI not found" >&2; exit 2; }
hcloud server list -l "ht-run=${RUN_ID}" -o json | python3 -c "
import json, re, sys
servers = json.load(sys.stdin)
if not servers:
    print('inventory: no servers labelled ht-run=${RUN_ID}', file=sys.stderr)
    sys.exit(2)
hosts = []
for s in servers:
    name = s['name']
    role = re.sub(r'^ht-${RUN_ID}-', '', name)
    role = re.sub(r'[0-9]+$', '', role)
    hosts.append({'name': name, 'role': role,
                  'public_ip': s['public_net']['ipv4']['ip'],
                  'private_ip': (s.get('private_net') or [{}])[0].get('ip', '')})
json.dump({'run_id': '${RUN_ID}', 'hosts': sorted(hosts, key=lambda h: h['name'])}, open('${OUT}', 'w'), indent=2)
print('inventory ->', '${OUT}', '(%d hosts)' % len(hosts))
"
