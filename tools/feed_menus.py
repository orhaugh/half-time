#!/usr/bin/env python3
"""Load the legacy menu system, and change the menus through the night.

    python3 tools/feed_menus.py --initial                         every menu, before the jobs start
    python3 tools/feed_menus.py --speed 30 --start-at <epoch s>   the night's changes, in step

The initial rows are what the change stream's snapshot finds. Each change is
one UPDATE in its own transaction, as the point-of-sale system commits it,
and the binary log carries the whole row.
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosts  # noqa: E402
import pacing  # noqa: E402

COLUMNS = ("restaurant_id", "item_id", "name", "price_cents", "available", "updated_at")


def q(v):
    return str(v) if isinstance(v, int) else "'" + str(v).replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--initial", action="store_true", help="load every menu as it stands before the night")
    ap.add_argument("--speed", type=float, default=30.0)
    ap.add_argument("--start-at", type=float, default=None, help="wall time of the first record")
    args = ap.parse_args()
    m = pacing.manifest()
    if args.initial:
        items = list(pacing.records("menus.jsonl"))
        sql = []
        for i in range(0, len(items), 200):
            values = ",".join("(" + ",".join(q(r[c]) for c in COLUMNS) + ")" for r in items[i:i + 200])
            sql.append(f"INSERT INTO menus ({','.join(COLUMNS)}) VALUES {values};")
        subprocess.run(hosts.mysql(), cwd=pacing.ROOT, input="\n".join(sql) + "\n", text=True, check=True)
        print(f"feed_menus: {len(items)} menu items -> the menu store")
        return
    client = subprocess.Popen(hosts.mysql(), cwd=pacing.ROOT, stdin=subprocess.PIPE, text=True)
    clock = pacing.Clock(args.start_at or time.time(), m["first_ms"], args.speed)
    sent, last_minute = 0, None
    for c in pacing.records("menu_changes.jsonl"):
        clock.wait_until(c["updated_at"])
        client.stdin.write(
            f"UPDATE menus SET name={q(c['name'])}, price_cents={c['price_cents']}, "
            f"available={c['available']}, updated_at={c['updated_at']} "
            f"WHERE restaurant_id={q(c['restaurant_id'])} AND item_id={q(c['item_id'])};\n")
        client.stdin.flush()
        sent += 1
        minute = clock.minute(c["updated_at"], m["t0_ms"])
        if minute != last_minute:
            pacing.progress("menus", minute)
            last_minute = minute
    client.stdin.close()
    rc = client.wait()
    if rc != 0:
        sys.exit(f"feed_menus: the mysql client exited with {rc}")
    print(f"feed_menus: {sent} menu changes -> the menu store")


if __name__ == "__main__":
    main()
