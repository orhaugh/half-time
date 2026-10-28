#!/usr/bin/env python3
"""Apply the night's orders to the order store, in step with the match.

    python3 tools/feed_orders.py --speed 30 --start-at <epoch seconds>

Every change in state/world/changes.jsonl becomes one statement in its own
transaction, as the platform's order service would commit it: an INSERT when
the order is placed, an UPDATE of its status as it moves on. clink reads the
table's replication stream, so this is the only place the orders enter.
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pacing  # noqa: E402

COLUMNS = ("order_id", "cell", "zone", "restaurant_id", "customer_id", "cx", "cy", "placed_at",
           "promised_at", "fee_cents", "total_cents", "status", "updated_at")


def psql():
    return subprocess.Popen(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-q", "-v", "ON_ERROR_STOP=1",
         "-U", "halftime", "-d", "halftime"],
        cwd=pacing.ROOT, stdin=subprocess.PIPE, text=True)


def literal(v):
    return str(v) if isinstance(v, int) else "'" + str(v).replace("'", "''") + "'"


def statement(c):
    if c["op"] == "insert":
        return (f"INSERT INTO orders ({', '.join(COLUMNS)}) VALUES "
                f"({', '.join(literal(c[k]) for k in COLUMNS)});\n")
    return (f"UPDATE orders SET status = {literal(c['status'])}, updated_at = {c['updated_at']} "
            f"WHERE order_id = {literal(c['order_id'])};\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--speed", type=float, default=30.0)
    ap.add_argument("--start-at", type=float, default=None)
    args = ap.parse_args()
    m = pacing.manifest()
    t0 = m["t0_ms"]
    clock = pacing.Clock(args.start_at or time.time(), m["first_ms"], args.speed)
    db = psql()
    applied, last_minute, last_flush = 0, None, time.time()
    for c in pacing.records("changes.jsonl"):
        clock.wait_until(c["updated_at"])
        db.stdin.write(statement(c))
        applied += 1
        now = time.time()
        if now - last_flush > 0.05:
            db.stdin.flush()
            last_flush = now
        minute = clock.minute(c["updated_at"], t0)
        if minute != last_minute:
            pacing.progress("orders", minute)
            last_minute = minute
    db.stdin.close()
    rc = db.wait()
    if rc != 0:
        sys.exit(f"feed_orders: psql exited with {rc}")
    print(f"feed_orders: {applied} order changes applied to the order store")


if __name__ == "__main__":
    main()
