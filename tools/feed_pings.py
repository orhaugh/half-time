#!/usr/bin/env python3
"""Send the riders' position reports to Kafka, in step with the match.

    python3 tools/feed_pings.py --speed 30 --start-at <epoch seconds>
    python3 tools/feed_pings.py --resend 2000      the 2,000 reports before
                                                   half-time, again
    python3 tools/feed_pings.py --shard 2/4 ...    one of four feeders, each
                                                   sending a quarter of the cities

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
import hosts  # noqa: E402
import pacing  # noqa: E402

T0 = None


def producer():
    return subprocess.Popen(
        hosts.kafka_tool("kafka-console-producer.sh", "--topic", "riders.positions",
                         "--producer-property", "enable.idempotence=true",
                         "--producer-property", "acks=all",
                         "--producer-property", "linger.ms=20"),
        cwd=pacing.ROOT, stdin=subprocess.PIPE, text=True)


def line(p):
    return json.dumps(p, separators=(",", ":")) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--speed", type=float, default=30.0)
    ap.add_argument("--start-at", type=float, default=None, help="wall time of the first record")
    ap.add_argument("--resend", type=int, default=0, help="send this many reports from before half-time again")
    ap.add_argument("--shard", default="0/1", help="this feeder's share of the cities, as i/n")
    args = ap.parse_args()
    m = pacing.manifest()
    t0 = m["t0_ms"]
    shard, shards = (int(x) for x in args.shard.split("/"))
    mine = {c for i, c in enumerate(m["cells"]) if i % shards == shard}
    prod = producer()
    sent = 0
    if args.resend:
        batch = [p for p in pacing.records("pings.jsonl") if p["ts"] < t0 + 45 * 60_000 and p["cell"] in mine][-args.resend:]
        for p in batch:
            prod.stdin.write(line(p))
        sent = len(batch)
    else:
        clock = pacing.Clock(args.start_at or time.time(), m["first_ms"], args.speed)
        last_minute, last_flush = None, time.time()
        for p in pacing.records("pings.jsonl"):
            if p["cell"] not in mine:
                continue
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
                pacing.progress("pings" if shards == 1 else f"pings-{shard}", minute)
                last_minute = minute
    prod.stdin.close()
    rc = prod.wait()
    if rc != 0:
        sys.exit(f"feed_pings: the producer exited with {rc}")
    print(f"feed_pings: {sent} position reports -> riders.positions"
          + (" (again)" if args.resend else ""))


if __name__ == "__main__":
    main()
