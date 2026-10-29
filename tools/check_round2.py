#!/usr/bin/env python3
"""Check the systems around the core against the reference model.

    python3 tools/check_round2.py [--wait SECONDS]

The menu stream is held to exactly once; the tablet bus, the kitchen
alerts, the desk's queue, the partners' topic, the floor's subject and the
order history to at least once, with the copies counted; the tracker's keys
to the latest state. --wait gives the tail that long to drain before the counts are
judged.
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
from check_run import Checks, latency_line, run, topic  # noqa: E402
from init_messaging import rabbitmq  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def pulsar_messages(name, seconds=30):
    """Every message of a topic, read from the start by a subscription of its own."""
    out = run(hosts.pulsar_client(seconds, "consume", "-s", f"check-{int(time.time())}", "-p", "Earliest",
                                  "-n", "0", f"persistent://public/default/{name}"), check=False)
    return [json.loads(line.split("content:", 1)[1]) for line in out.splitlines() if "content:" in line]


def rabbitmq_messages(queue, count):
    """Up to count messages of a queue, read and requeued, so they stay."""
    got = rabbitmq("POST", f"/api/queues/%2F/{queue}/get",
                   {"count": count, "ackmode": "ack_requeue_true", "encoding": "auto"})
    return [json.loads(m["payload"]) for m in got]


def nats_messages(stream, subject):
    """Every message a JetStream stream holds for a subject, and how many it holds."""
    info = json.loads(run(hosts.nats("stream", "info", stream, "--json")))
    n = info["state"]["messages"]
    out = run(hosts.nats("sub", subject, "--stream", stream, "--all", f"--count={n}"), check=False) if n else ""
    return n, [json.loads(line) for line in out.splitlines() if line.startswith("{")]


def redis_values(prefix):
    """key suffix -> the row, for every key with the prefix."""
    keys = [k for k in run(hosts.redis_cli("--raw", "KEYS", prefix + "*")).splitlines() if k.strip()]
    out = {}
    for i in range(0, len(keys), 500):
        chunk = keys[i:i + 500]
        values = run(hosts.redis_cli("--raw", "MGET", *chunk)).splitlines()
        for k, v in zip(chunk, values):
            out[k[len(prefix):]] = json.loads(v)
    return out


def cassandra_rows():
    out = run(hosts.cqlsh("-e", "COPY halftime.order_history (order_id, updated_at, status) TO STDOUT"), check=False)
    rows = []
    for line in out.splitlines():
        parts = line.strip().split(",")
        if len(parts) == 3 and parts[1].isdigit():
            rows.append((parts[0], parts[2], int(parts[1])))
    return rows


def mysql_rows(sql):
    return [line.split("\t") for line in run(hosts.mysql("-N", "-B", "-e", sql)).splitlines() if line.strip()]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--wait", type=int, default=0, help="seconds to give the pipeline's tail")
    args = ap.parse_args()
    ref = reference.Reference()
    c = Checks(ref)
    menu_stream = ref.menu_stream()
    menu_final = ref.menu_final()
    alerts = ref.alerts()
    late_alerts = ref.late_alerts()
    decisions = ref.decisions()
    late_window = ref.late_window()
    escalated = {k for k, v in decisions.items() if v[0] == "escalated"}
    assigned = {k: v[1] for k, v in decisions.items() if v[0] == "assigned"}
    final = {o["order_id"]: ("cancelled" if o["cancelled_at"] else "delivered") for o in ref.orders}
    expected_changes = Counter((ch["order_id"], ch["status"], ch["updated_at"]) for ch in ref.changes)
    third_bounce = {(e["restaurant_id"], e["order_id"], e["ts"]): e["event_id"] for e in ref.kitchen}

    # Wait for the tail: the desk's queue and the tracker's rider keys fill as
    # the dispatcher decides, last of all.
    want_keys = len(final) + sum(1 for k in assigned if k not in late_window)
    want_desk = sum(1 for k in escalated if k not in late_window)
    deadline = time.time() + args.wait
    while time.time() < deadline:
        keys = int(run(hosts.redis_cli("DBSIZE")).split()[-1])
        desk = rabbitmq("GET", "/api/queues/%2F/ops.escalations").get("messages", 0)
        if keys >= want_keys and desk >= want_desk:
            break
        time.sleep(5)

    print("the menu stream: the menu store's rows as the night began, then every change, exactly once")
    ms = topic("menus.changes")
    c.exactly_once("menus.changes topic", [(r["restaurant_id"], r["item_id"], r["updated_at"], r["op"]) for r in ms],
                   menu_stream)
    wrong = [r for r in ms if menu_stream.get((r["restaurant_id"], r["item_id"], r["updated_at"], r["op"])) !=
             (r["name"], r["price_cents"], r["available"])]
    c.judge(f"menu values: {len(ms) - len(wrong)} of {len(ms)} as the model says", not wrong)
    store = mysql_rows("SELECT restaurant_id, item_id, name, price_cents, available, updated_at FROM menus")
    got = {(r[0], r[1]): (r[2], int(r[3]), int(r[4]), int(r[5])) for r in store}
    c.judge(f"menu store: {len(store)} items, {sum(1 for k, v in got.items() if menu_final.get(k) == v)} "
            f"at their final state, expected {len(menu_final)}", got == menu_final)

    print("the tablet bus: every kitchen event carried onto Pulsar, at least once")
    bus = pulsar_messages("kitchen.events")
    c.at_least_once("kitchen.events topic (Pulsar)", [r["event_id"] for r in bus], {e["event_id"] for e in ref.kitchen})

    print("the kitchen watch: three bounces in a row at a restaurant raise an alert, at least once")
    al = topic("kitchen.alerts")
    c.at_least_once("kitchen.alerts topic", [(r["restaurant_id"], r["first_at"]) for r in al], alerts, late_alerts)
    wrong = [r for r in al if alerts.get((r["restaurant_id"], r["first_at"])) !=
             (r["cell"], r["first_order"], r["last_order"], r["last_at"])]
    c.judge(f"alert values: {len(al) - len(wrong)} of {len(al)} as the model says", not wrong)

    print("the operations desk, the partners and the floor: each escalation and each alert delivered, at least once")
    desk = rabbitmq_messages("ops.escalations", 3 * len(escalated) + 100)
    c.at_least_once("ops.escalations queue (RabbitMQ)", [r["order_id"] for r in desk], escalated, late_window)
    partners = pulsar_messages("partners.escalations")
    c.at_least_once("partners.escalations topic (Pulsar)", [r["order_id"] for r in partners], escalated, late_window)
    n, floor = nats_messages("ALERTS", "ops.alerts")
    c.at_least_once(f"ops.alerts subject (NATS; the stream holds {n})",
                    [(r["restaurant_id"], r["first_at"]) for r in floor], alerts, late_alerts)

    print("the live tracker: every order at its latest state, its rider, and its history")
    tr = redis_values("order:")
    got_status = {k: v["status"] for k, v in tr.items()}
    c.judge(f"order: keys (Redis): {len(tr)} orders, {sum(1 for k, v in got_status.items() if final.get(k) == v)} "
            f"at their final state, expected {len(final)}", got_status == final)
    ro = redis_values("rider-of:")
    got_rider = {k: v["rider_id"] for k, v in ro.items()}
    want_rider = {k: v for k, v in assigned.items() if k not in late_window}
    ok = all(got_rider.get(k) == v for k, v in want_rider.items()) and \
        all(assigned.get(k) == v for k, v in got_rider.items())
    c.judge(f"rider-of: keys (Redis): {len(ro)} orders with a rider, "
            f"{sum(1 for k, v in got_rider.items() if assigned.get(k) == v)} as the model says, "
            f"expected {len(want_rider)} and up to {len(assigned)}", ok)
    hist = cassandra_rows()
    c.at_least_once("order_history table (Cassandra)", hist, expected_changes)

    print("how long the kitchen path took, wall clock, from the third bounce reaching Kafka:")
    seen = {}
    for r in topic("kitchen.events"):
        seen.setdefault(r["event_id"], r["appended_ms"])
    latency_line("alert published", [r["appended_ms"] - seen[third_bounce[(r["restaurant_id"], r["last_order"], r["last_at"])]]
                                     for r in al if (r["restaurant_id"], r["first_at"]) not in late_alerts
                                     and (r["restaurant_id"], r["last_order"], r["last_at"]) in third_bounce])
    print("check_round2:", "PASS" if c.ok else "FAIL")
    sys.exit(0 if c.ok else 1)


if __name__ == "__main__":
    main()
