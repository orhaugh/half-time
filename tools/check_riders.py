#!/usr/bin/env python3
"""Check the riders job, the typed C++ one: the rider app's store in MongoDB
and the commands it sent the riders over MQTT.

    python3 tools/check_riders.py [--wait SECONDS]

The store is held to one document per rider, the rider's latest status as
the model says: the job keeps a rider's newest report by the phone's clock,
and the phones' topics are retained, so a restart that lost reports in flight
is handed every rider's latest again. The commands are at least once, every
low-battery episode of the model's answered by one, and each answering a
report below the threshold in its episode, with the battery the model gives.
clink's MQTT source acknowledges a message to the broker once it has read
it, so a report taken in and lost with a killed Worker before the next
checkpoint is not handed over again; an episode survives that because a
battery stays low for several reports, and the next one raises the command.
"""
import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosts  # noqa: E402
import reference  # noqa: E402
import world as W  # noqa: E402
from check_run import Checks  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
COMMANDS = ROOT / "state" / "run" / "commands.txt"


def store():
    """rider_id -> the document the rider app's store holds."""
    out = subprocess.run(
        hosts.mongosh("--eval", 'db.getSiblingDB("riderapp").riders.find().forEach(d => print(EJSON.stringify(d, {relaxed: true})))'),
        cwd=ROOT, capture_output=True, text=True, check=True).stdout
    docs = [json.loads(line) for line in out.splitlines() if line.startswith("{")]
    return {d["rider_id"]: d for d in docs}


def commands():
    """The commands the recorder took off riders/commands, in order."""
    if not COMMANDS.exists():
        return []
    out = []
    for line in COMMANDS.read_text().splitlines():
        topic, _, payload = line.partition(" ")
        if topic == "riders/commands" and payload.startswith("{"):
            out.append(json.loads(payload))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--wait", type=int, default=0, help="seconds to give the job's tail")
    args = ap.parse_args()
    ref = reference.Reference()
    c = Checks(ref)
    latest = ref.riders_latest()
    episodes = ref.low_battery_episodes()
    reports = {(d["rider_id"], d["ts"]): d for d in ref.devices}

    # The tail: the last reports reach the store at the job's next flush.
    deadline = time.time() + args.wait
    docs = store()
    while time.time() < deadline:
        if len(docs) >= len(latest) and all(docs.get(r, {}).get("ts") == d["ts"] for r, d in latest.items()):
            break
        time.sleep(5)
        docs = store()

    print("the rider app's store: each rider's latest status, by the phone's clock")
    c.exactly_once("riders collection (MongoDB), by rider", list(docs), latest)
    wrong = [r for r, d in docs.items()
             if r in latest and (d.get("ts"), d.get("battery_pct"), d.get("state"), d.get("cell"))
             != (latest[r]["ts"], latest[r]["battery_pct"], latest[r]["state"], latest[r]["cell"])]
    c.judge(f"document values: {len(docs) - len(wrong)} of {len(docs)} as the model says", not wrong)

    print("the commands: one to charge per low-battery episode, at least once")
    cmds = commands()

    def episode_of(cmd):
        rider, ts = cmd.get("rider_id"), cmd.get("ts")
        report = reports.get((rider, ts))
        if report is None or report["battery_pct"] >= W.LOW_BATTERY_PCT or cmd.get("battery_pct") != report["battery_pct"]:
            return None
        for (r, start), until in episodes.items():
            if r == rider and start <= ts and (until is None or ts < until):
                return (r, start)
        return None

    answered = Counter(episode_of(cmd) for cmd in cmds)
    spurious = answered.pop(None, 0)
    copies = sum(answered.values()) - len(answered)
    c.judge(f"riders/commands topic (MQTT): {len(cmds)} commands, {spurious} answering no low-battery "
            f"report of the model's", spurious == 0 and len(cmds) > 0)
    c.judge(f"low-battery episodes commanded: {len(answered)} of {len(episodes)}, {copies} repeated "
            f"(at least once)", len(answered) == len(episodes))
    print("check_riders:", "PASS" if c.ok else "FAIL")
    sys.exit(0 if c.ok else 1)


if __name__ == "__main__":
    main()
