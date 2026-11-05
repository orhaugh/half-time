#!/usr/bin/env bash
# Run half-time against one clink release candidate and keep the evidence of
# what ran: which clink and which half-time, how the run was invoked, what each
# fault scene did, every checker's output, exit code and verdicts, and the logs
# around the faults. clink's scripts/check-showcases.sh takes the bundle for
# its candidate (see the README, "Release evidence").
#
#   CLINK_SRC=<clink checkout of the candidate> ./evidence.sh <candidate commit>
#
# CLINK_SRC must be exactly the candidate with nothing changed, and its build/
# (or CLINK_BIN_DIR) built from a fresh configure there, since clink records
# its commit when it is configured: tools/evidence.py refuses anything else
# before the run starts. The run is run.sh's with every scene, so SCENES must
# be unset or 1; SPEED, DISPATCH_PARALLELISM, TRACE_CHECK and CAPTURE_JOBS pass
# through and are recorded. The bundle goes to evidence/<first 12 of the
# candidate>-<UTC time>/ (or EVIDENCE_DIR). It leaves out the checkpoints, the
# HA directory and every credential, and MANIFEST.sha256 lists each file's hash.
set -euo pipefail
cd "$(dirname "$0")"

[ $# = 1 ] || { echo "usage: CLINK_SRC=<clink checkout of the candidate> ./evidence.sh <candidate commit>" >&2; exit 2; }
: "${CLINK_SRC:?set CLINK_SRC to a clink checkout of the candidate}"
CLINK_SRC="$(cd "${CLINK_SRC}" && pwd)"
CLINK_BIN_DIR="${CLINK_BIN_DIR:-${CLINK_SRC}/build}"
[ "${SCENES:-1}" = 1 ] || { echo "evidence.sh: the evidence needs every scene; unset SCENES" >&2; exit 2; }
candidate="$(git -C "${CLINK_SRC}" rev-parse --verify "$1^{commit}")"
out="${EVIDENCE_DIR:-evidence/${candidate:0:12}-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${out}"

python3 -I tools/evidence.py preflight --candidate "${candidate}" --clink-src "${CLINK_SRC}" \
  --bin-dir "${CLINK_BIN_DIR}" --out "${out}"
started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
rc=0
SCENES=1 CLINK_SRC="${CLINK_SRC}" CLINK_BIN_DIR="${CLINK_BIN_DIR}" ./run.sh 2>&1 | tee "${out}/run.log" || rc=$?
python3 -I tools/evidence.py collect --out "${out}" --run-exit "${rc}" --started "${started}"
echo "Judge it with clink's scripts/check-half-time-evidence.py ${out} --candidate ${candidate}"
exit "${rc}"
