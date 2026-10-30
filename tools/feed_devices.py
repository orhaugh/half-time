#!/usr/bin/env python3
"""Play the riders' phones: every status report, at the night's pace, each to
its rider's own topic, retained.

    python3 tools/feed_devices.py --speed 30 --start-at <wall time>

The broker keeps a retained message as its topic's latest, so a subscriber
that comes back, the riders job after a restart, is handed every rider's
latest status whatever it missed while it was away.
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosts  # noqa: E402
import mqtt  # noqa: E402
import pacing  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--speed", type=float, default=30.0)
    ap.add_argument("--start-at", type=float, default=None, help="wall time of the first record")
    args = ap.parse_args()
    m = pacing.manifest()
    host, port = hosts.mqtt_address()
    client = mqtt.Client(host, port, "halftime-devices")
    clock = pacing.Clock(args.start_at or time.time(), m["first_ms"], args.speed)
    sent, last_minute = 0, None
    for d in pacing.records("devices.jsonl"):
        clock.wait_until(d["ts"])
        client.publish(f"riders/{d['rider_id']}/status", json.dumps(d, separators=(",", ":")), retain=True)
        sent += 1
        minute = clock.minute(d["ts"], m["t0_ms"])
        if minute != last_minute:
            pacing.progress("devices", minute)
            last_minute = minute
    client.close()
    print(f"feed_devices: {sent} status reports -> riders/<id>/status, retained")


if __name__ == "__main__":
    main()
