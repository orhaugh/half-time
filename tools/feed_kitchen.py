#!/usr/bin/env python3
"""Send the restaurant tablets' events to Kafka, in step with the match.

    python3 tools/feed_kitchen.py --speed 30 --start-at <epoch seconds>
    python3 tools/feed_kitchen.py --resend 500       the 500 events before
                                                     half-time, again
    python3 tools/feed_kitchen.py --shard 2/4 ...    one of four feeders

The tablet vendor's bus is ordered, so the topic has one partition and the
producer is idempotent: the events reach Kafka in the order the tablets sent
them, and the bridge job carries them onto Pulsar in that order. A resend is
what the vendor's producer does after a retry: the same events again.
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


def producer():
    return subprocess.Popen(
        hosts.kafka_tool("kafka-console-producer.sh", "--topic", "kitchen.events",
                         "--producer-property", "enable.idempotence=true",
                         "--producer-property", "acks=all",
                         "--producer-property", "linger.ms=20"),
        cwd=pacing.ROOT, stdin=subprocess.PIPE, text=True)


def line(e):
    return json.dumps(e, separators=(",", ":")) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--speed", type=float, default=30.0)
    ap.add_argument("--start-at", type=float, default=None, help="wall time of the first record")
    ap.add_argument("--resend", type=int, default=0, help="send this many events from before half-time again")
    ap.add_argument("--shard", default="0/1", help="this feeder's share of the cities, as i/n")
    args = ap.parse_args()
    m = pacing.manifest()
    t0 = m["t0_ms"]
    shard, shards = (int(x) for x in args.shard.split("/"))
    mine = {c for i, c in enumerate(m["cells"]) if i % shards == shard}
    prod = producer()
    sent = 0
    if args.resend:
        batch = [e for e in pacing.records("kitchen.jsonl")
                 if e["ts"] < t0 + 45 * 60_000 and e["cell"] in mine][-args.resend:]
        for e in batch:
            prod.stdin.write(line(e))
        sent = len(batch)
    else:
        clock = pacing.Clock(args.start_at or time.time(), m["first_ms"], args.speed)
        last_minute, last_flush = None, time.time()
        for e in pacing.records("kitchen.jsonl"):
            if e["cell"] not in mine:
                continue
            clock.wait_until(e["ts"])
            e["sent_at"] = int(time.time() * 1000)
            prod.stdin.write(line(e))
            sent += 1
            now = time.time()
            if now - last_flush > 0.05:
                prod.stdin.flush()
                last_flush = now
            minute = clock.minute(e["ts"], t0)
            if minute != last_minute:
                pacing.progress("kitchen" if shards == 1 else f"kitchen-{shard}", minute)
                last_minute = minute
    prod.stdin.close()
    rc = prod.wait()
    if rc != 0:
        sys.exit(f"feed_kitchen: the producer exited with {rc}")
    print(f"feed_kitchen: {sent} tablet events -> kitchen.events" + (" (again)" if args.resend else ""))


if __name__ == "__main__":
    main()
