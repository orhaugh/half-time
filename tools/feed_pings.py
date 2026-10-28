#!/usr/bin/env python3
"""Send the riders' position reports to Kafka, in step with the match.

    python3 tools/feed_pings.py --speed 30 --start-at <epoch seconds>
    python3 tools/feed_pings.py --resend 2000      the 2,000 reports before
                                                   half-time, again

The producer is Kafka's own, inside the broker container, with idempotence
on, as a rider app's backend would send them; the reports carry no key, so
they spread over the topic's partitions and every partition keeps flowing. A
resend is what a producer that retried a batch it had delivered does: the
same records again.
"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pacing  # noqa: E402

T0 = None


def producer():
    return subprocess.Popen(
        ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", "kafka:9092", "--topic", "riders.positions",
         "--producer-property", "enable.idempotence=true", "--producer-property", "acks=all",
         "--producer-property", "linger.ms=20"],
        cwd=pacing.ROOT, stdin=subprocess.PIPE, text=True)


def line(p):
    return json.dumps(p, separators=(",", ":")) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--speed", type=float, default=30.0)
    ap.add_argument("--start-at", type=float, default=None, help="wall time of the first record")
    ap.add_argument("--resend", type=int, default=0, help="send this many reports from before half-time again")
    args = ap.parse_args()
    m = pacing.manifest()
    t0 = m["t0_ms"]
    prod = producer()
    sent = 0
    if args.resend:
        batch = [p for p in pacing.records("pings.jsonl") if p["ts"] < t0 + 45 * 60_000][-args.resend:]
        for p in batch:
            prod.stdin.write(line(p))
        sent = len(batch)
    else:
        clock = pacing.Clock(args.start_at or time.time(), m["first_ms"], args.speed)
        last_minute, last_flush = None, time.time()
        for p in pacing.records("pings.jsonl"):
            clock.wait_until(p["ts"])
            p["sent_at"] = int(time.time() * 1000)
            prod.stdin.write(line(p))
            sent += 1
            now = time.time()
            if now - last_flush > 0.05:
                prod.stdin.flush()
                last_flush = now
            minute = clock.minute(p["ts"], t0)
            if minute != last_minute:
                pacing.progress("pings", minute)
                last_minute = minute
    prod.stdin.close()
    rc = prod.wait()
    if rc != 0:
        sys.exit(f"feed_pings: the producer exited with {rc}")
    print(f"feed_pings: {sent} position reports -> riders.positions"
          + (" (again)" if args.resend else ""))


if __name__ == "__main__":
    main()
