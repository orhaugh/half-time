#!/usr/bin/env python3
"""Check the search index, the match feed's outputs, the lakehouse, the
telemetry store, the gauges and the backfill against the reference model.

    python3 tools/check_round3.py [--wait SECONDS]

Each is written at least once and collapses its copies by key, so each is
held to every expected row present, the copies counted, and the values the
model computes.
"""
import argparse
import base64
import csv
import io
import json
import sys
import time
import urllib.request
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosts  # noqa: E402
import reference  # noqa: E402
from check_run import Checks, ch, topic  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def http(method, url, body=None, headers=None, raw=False):
    data = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=120) as resp:
        text = resp.read().decode()
    return text if raw else (json.loads(text) if text else None)


def opensearch(method, path, body=None):
    return http(method, hosts.opensearch_url() + path, body, {"Content-Type": "application/json"})


def influx_count(measurement, field):
    flux = (f'from(bucket: "{hosts.INFLUX_BUCKET}") |> range(start: 0) '
            f'|> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{field}") '
            '|> group() |> count()')
    text = http("POST", f"{hosts.influxdb_url()}/api/v2/query?org={hosts.INFLUX_ORG}", flux.encode(),
                {"Authorization": f"Token {hosts.INFLUX_TOKEN}", "Content-Type": "application/vnd.flux",
                 "Accept": "application/csv"}, raw=True)
    for row in csv.DictReader(io.StringIO(text)):
        if row.get("_value"):
            return int(row["_value"])
    return 0


def pushgateway_gauges(name):
    """zone -> (cell, window_end, value) from the gateway's exposition."""
    out = {}
    for line in http("GET", f"{hosts.pushgateway_url()}/metrics", raw=True).splitlines():
        if not line.startswith(name + "{"):
            continue
        labels, value = line[len(name) + 1:].rsplit("} ", 1)
        kv = dict(part.split("=", 1) for part in labels.split(","))
        kv = {k: v.strip('"') for k, v in kv.items()}
        out[kv["zone"]] = (kv["cell"], int(kv["window_end"]), int(float(value)))
    return out


def delta_table(rel):
    """The data files the Delta log has committed, and the log's version count."""
    log = sorted((ROOT / "state" / "archive" / rel / "_delta_log").glob("*.json"))
    files = []
    for version in log:
        for line in version.read_text().splitlines():
            if line.strip():
                action = json.loads(line)
                if "add" in action:
                    files.append(action["add"]["path"])
                if "remove" in action:
                    files.remove(action["remove"]["path"])
    return files, len(log)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--wait", type=int, default=0, help="seconds to give the pipeline's tail")
    args = ap.parse_args()
    ref = reference.Reference()
    c = Checks(ref)
    menu_stream = ref.menu_stream()
    goal_orders = ref.goal_orders()
    expected_changes = Counter((x["order_id"], x["status"], x["updated_at"]) for x in ref.changes)
    decisions = topic("dispatch.decisions")
    want_decisions = {r["order_id"]: (r["kind"], r["rider_id"]) for r in decisions}

    # Wait for the tail: the backfill is the last job to run, and the goal
    # join waits on the order stream's watermark.
    deadline = time.time() + args.wait
    while time.time() < deadline:
        n = ch("SELECT count() AS n FROM halftime.dispatch_decisions_backfill")[0]["n"]
        g = ch("SELECT count() AS n FROM halftime.goal_orders")[0]["n"]
        if n >= len(want_decisions) and g >= len(goal_orders):
            break
        time.sleep(5)

    print("the menu search: every change to a menu, one document each")
    opensearch("POST", "/menus/_refresh")
    hits = opensearch("GET", "/menus/_search?size=5000")["hits"]["hits"]
    docs = {h["_id"]: h["_source"] for h in hits}
    want_docs = {f"{r}:{i}:{t}": v for (r, i, t, _op), v in menu_stream.items()}
    c.exactly_once("menus index (OpenSearch), by document", list(docs), want_docs)
    wrong = [d for d, s in docs.items() if want_docs.get(d) != (s["name"], s["price_cents"], s["available"])]
    c.judge(f"document values: {len(docs) - len(wrong)} of {len(docs)} as the model says", not wrong)

    print("the match feed: every event announced exactly once and kept, and each goal's orders")
    me = topic("match.events")
    want_events = {e["seq"]: (e["kind"], e["minute"], e["ts"]) for e in ref.match}
    c.exactly_once("match.events topic", [r["seq"] for r in me], want_events)
    wrong = [r for r in me if want_events.get(r["seq"]) != (r["kind"], r["minute"], r["ts"])]
    c.judge(f"event values: {len(me) - len(wrong)} of {len(me)} as the feed says", not wrong)
    tl = ch("SELECT seq, kind, minute, ts_ms FROM halftime.match_timeline")
    c.at_least_once("match_timeline (ClickHouse)", [r["seq"] for r in tl], want_events)
    go = ch("SELECT goal_seq, order_id, goal_minute, cell, placed_at_ms FROM halftime.goal_orders")
    c.at_least_once("goal_orders (ClickHouse)", [(r["goal_seq"], r["order_id"]) for r in go], goal_orders)
    wrong = [r for r in go if goal_orders.get((r["goal_seq"], r["order_id"])) != (r["goal_minute"], r["cell"], r["placed_at_ms"])]
    per_goal = Counter(k[0] for k in goal_orders)
    c.judge(f"goal_orders values: {len(go) - len(wrong)} of {len(go)} as the model says; the model's surge per goal: "
            + ", ".join(f"minute {ref.match[s - 1]['minute']}: {n}" for s, n in sorted(per_goal.items())), not wrong)

    print("the lakehouse: the order stream as a Delta table, read back by ClickHouse")
    files, versions = delta_table("lake/orders.changes")
    if files:
        glob = "{" + ",".join(files) + "}" if len(files) > 1 else files[0]
        rows = ch(f"SELECT order_id, status, updated_at FROM file('archive/lake/orders.changes/{glob}', Parquet)")
        c.at_least_once(f"lake/orders.changes ({versions} commits, {len(files)} data files)",
                        [(r["order_id"], r["status"], r["updated_at"]) for r in rows], expected_changes)
    else:
        c.judge("lake/orders.changes: no committed data files", False)

    print("the telemetry store: every position report as a point")
    n = influx_count("rider_position", "x")
    c.judge(f"rider_position (InfluxDB): {n} points, expected {len(ref.pings)} (one per report; a report written "
            f"twice is the same point)", n == len(ref.pings), guarantee="upsert")

    print("the gauges: each zone's surge multiplier on the Pushgateway, at its latest window")
    mult = topic("pricing.multipliers")
    by_window = {(r["cell"], r["zone"], r["window_end"]): r["multiplier_pct"] for r in mult}
    latest = {}
    for (cell, zone, we), _v in by_window.items():
        if zone not in latest or we > latest[zone]:
            latest[zone] = we
    gauges = pushgateway_gauges("halftime_zone_multiplier_pct")
    consistent = all(by_window.get((cell, zone, we)) == v for zone, (cell, we, v) in gauges.items())
    behind = {zone: (latest.get(zone, we) - we) // 60_000 for zone, (_cell, we, _v) in gauges.items()}
    # The gateway keeps the last push only, and a push carries the series of
    # the records since the one before, so a zone whose window closed a
    # little earlier can be absent from the final exposition.
    c.judge(f"halftime_zone_multiplier_pct: {len(gauges)} of {len(latest)} zones in the last push, each with "
            f"the topic's value for the window it names; {sum(1 for b in behind.values() if b == 0)} at the "
            f"latest window, the rest within {max(behind.values()) if behind else 0} windows of it",
            consistent and gauges and len(gauges) <= len(latest) and all(b <= 2 for b in behind.values()),
            guarantee="upsert")

    print("the backfill: the dispatch archive read back beside the live table")
    bf = ch("SELECT order_id, kind, rider_id FROM halftime.dispatch_decisions_backfill")
    c.at_least_once("dispatch_decisions_backfill (ClickHouse)", [r["order_id"] for r in bf], want_decisions)
    wrong = [r for r in bf if want_decisions.get(r["order_id"]) != (r["kind"], r["rider_id"])]
    c.judge(f"backfill values: {len(bf) - len(wrong)} of {len(bf)} as the topic says", not wrong)
    print("check_round3:", "PASS" if c.ok else "FAIL")
    sys.exit(0 if c.ok else 1)


if __name__ == "__main__":
    main()
