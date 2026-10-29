#!/usr/bin/env bash
# Remove a rig by label, and prove nothing is left.
#
#   ./rig/teardown.sh <run id>          delete that run's servers, firewall and network
#   ./rig/teardown.sh <run id> --list   list what would be deleted, delete nothing
#   ./rig/teardown.sh --check           fail if anything labelled ht=1 survives in the project
#   ./rig/teardown.sh --all             delete everything labelled ht=1, the ssh key included
#
# Orphaned infrastructure bills until it is found. Every resource the rig
# creates carries ht=1 and ht-run=<run id>; this is the one teardown path,
# and --check is the end of every session.
set -euo pipefail
command -v hcloud >/dev/null 2>&1 || { echo "teardown: hcloud CLI not found" >&2; exit 2; }
case "${1:-}" in
  --check)
    left=0
    for kind in server volume load-balancer firewall network ssh-key; do
      while IFS= read -r line; do
        [ -n "${line}" ] || continue
        echo "LEFTOVER ${kind}: ${line}" >&2
        left=1
      done < <(hcloud "${kind}" list -l ht=1 -o noheader -o columns=id,name 2>/dev/null || true)
    done
    [ "${left}" = 0 ] && echo "teardown --check: nothing labelled ht=1 survives" || { echo "teardown --check: RESOURCES SURVIVE and bill until destroyed" >&2; exit 1; }
    ;;
  --all|?*)
    if [ "$1" = "--all" ]; then selector="ht=1"; else selector="ht-run=$1"; fi
    list_only=0; [ "${2:-}" = "--list" ] && list_only=1
    for kind in server volume firewall network ssh-key; do
      [ "${kind}" = ssh-key ] && [ "${selector}" != "ht=1" ] && continue
      while IFS= read -r id; do
        [ -n "${id}" ] || continue
        if [ "${list_only}" = 1 ]; then echo "would delete ${kind} ${id} (${selector})"; else echo "deleting ${kind} ${id}"; hcloud "${kind}" delete "${id}"; fi
      done < <(hcloud "${kind}" list -l "${selector}" -o noheader -o columns=id 2>/dev/null || true)
    done
    ;;
  *)
    echo "usage: rig/teardown.sh <run id> [--list] | --check | --all" >&2; exit 2 ;;
esac
