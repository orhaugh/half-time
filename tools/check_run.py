#!/usr/bin/env python3
"""Check every store a run writes against the reference model.

    python3 tools/check_run.py [--wait SECONDS]

Each store is held to the guarantee its table declares: a topic or table
written exactly once must hold the expected rows and nothing twice; a store
written at least once must hold every expected row, and the copies are
counted; a table kept by upsert must show the latest state. --wait gives the
pipeline's tail that long to drain before the counts are judged.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosts  # noqa: E402
import reference  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
LAG = 60_000


def run(argv, check=True):
    return subprocess.run(argv, cwd=ROOT, check=check, capture_output=True, text=True).stdout


def topic(name):
    """Every committed record of a topic with the broker's LogAppendTime."""
    out = run(hosts.kafka_tool("kafka-console-consumer.sh", "--topic", name, "--from-beginning",
                               "--isolation-level", "read_committed", "--timeout-ms", "8000",
                               "--property", "print.timestamp=true",
                               "--property", "print.partition=true"), check=False)
    rows = []
    for line in out.splitlines():
        if not line.startswith(("LogAppendTime:", "CreateTime:")):
            continue
        stamp, partition, payload = line.split("\t", 2)
        row = json.loads(payload)
        row["appended_ms"] = int(stamp.split(":", 1)[1])
        row["partition"] = int(partition.split(":", 1)[1])
        rows.append(row)
    return rows


def partition_floor(rows, column):
    """Where a stream's watermark stops when the stream ends: the lowest of its
    partitions' last event times, less the lag. A row behind it must have been
    seen; a row past it may still be waiting for a partition that ended early."""
    last = defaultdict(int)
    for r in rows:
        last[r["partition"]] = max(last[r["partition"]], r[column])
    return min(last.values()) - LAG if last else 0


def topic_count(name):
    """How many records a topic holds, from its end offsets: for a stream too
    large to read back, written by a producer that is not transactional."""
    out = run(hosts.kafka_tool("kafka-get-offsets.sh", "--topic", name), check=False)
    return sum(int(line.rsplit(":", 1)[1]) for line in out.splitlines() if line.strip())


def pg(sql):
    out = run(hosts.psql("-At", "-c", f"SELECT row_to_json(x) FROM ({sql}) x"))
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def ch(sql):
    out = run(hosts.clickhouse_client("--format", "JSONEachRow",
                                      "--output_format_json_quote_64bit_integers", "0",
                                      "--query", sql), check=False)
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def verdict(ok):
    return "  OK" if ok else "  FAIL"


def pct(values, f):
    if not values:
        return 0
    s = sorted(values)
    return s[min(len(s) - 1, int(f * len(s)))]


def latency_line(label, values):
    print(f"    {label:<30} p50 {pct(values, .5):6.0f} p90 {pct(values, .9):6.0f} "
          f"p99 {pct(values, .99):6.0f} max {max(values) if values else 0:6.0f} ms  (n={len(values)})")


class Checks:
    def __init__(self, ref):
        self.ref = ref
        self.ok = True

    def judge(self, line, ok):
        self.ok = self.ok and ok
        print("  " + line + verdict(ok))

    def exactly_once(self, what, got_keys, expected_keys, optional=frozenset()):
        """Rows keyed for identity: every expected key once, nothing else. An
        optional key (a window the night ended inside) may be absent, and is
        held to once when present."""
        counts = Counter(got_keys)
        missing = [k for k in expected_keys if k not in counts and k not in optional]
        extra = [k for k in counts if k not in expected_keys and k not in optional]
        twice = sum(n - 1 for n in counts.values() if n > 1)
        ok = not missing and not extra and twice == 0
        detail = f"{len(got_keys)} rows for {len(expected_keys)} expected"
        if optional:
            detail += f" ({sum(1 for k in optional if k in counts)} of {len(optional)} open at the end present)"
        if missing:
            detail += f", {len(missing)} missing"
        if extra:
            detail += f", {len(extra)} unexpected"
        if twice:
            detail += f", {twice} repeated"
        self.judge(f"{what}: {detail}", ok)
        return ok

    def at_least_once(self, what, got_keys, expected_keys, optional=frozenset()):
        counts = Counter(got_keys)
        missing = [k for k in expected_keys if k not in counts and k not in optional]
        extra = [k for k in counts if k not in expected_keys and k not in optional]
        twice = sum(n - 1 for n in counts.values() if n > 1)
        ok = not missing and not extra
        detail = f"{len(got_keys)} rows, {len(counts)} distinct, {len(expected_keys)} expected"
        if missing:
            detail += f", {len(missing)} missing"
        if extra:
            detail += f", {len(extra)} unexpected"
        detail += f", {twice} repeated" if twice else ", none repeated"
        self.judge(f"{what}: {detail}", ok)
        return ok


def order_changes_keys(rows):
    return [(r["order_id"], r["status"], r["updated_at"]) for r in rows]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--wait", type=int, default=0, help="seconds to give the pipeline's tail")
    ap.add_argument("--scale", action="store_true",
                    help="a large night: count the position reports from the topic's offsets "
                         "instead of reading them back")
    args = ap.parse_args()
    ref = reference.Reference()
    c = Checks(ref)
    expected_changes = Counter(order_changes_keys(ref.changes))
    windows = ref.windows()
    quotes = ref.quotes()
    decisions = ref.decisions()
    late_window = ref.late_window()
    payouts = ref.payouts()
    late_payouts = {k for k in payouts if k in late_window}
    credits = Counter(ref.credits())

    # Wait for the tail: the ledgers are the last to fill.
    deadline = time.time() + args.wait
    while time.time() < deadline:
        n_pay = pg("SELECT count(*) AS n FROM payouts")[0]["n"]
        n_cred = pg("SELECT count(*) AS n FROM credits")[0]["n"]
        if n_pay >= len(payouts) - len(late_payouts) and n_cred >= sum(credits.values()):
            break
        time.sleep(5)

    print("the order stream: every change the order store committed, exactly once")
    changes = topic("orders.changes")
    c.exactly_once("orders.changes topic", order_changes_keys(changes), expected_changes)
    final = {o["order_id"]: o for o in ref.orders}
    store = pg("SELECT order_id, status, updated_at FROM orders")
    want_final = {oid: ("cancelled" if o["cancelled_at"] else "delivered") for oid, o in final.items()}
    got_final = {r["order_id"]: r["status"] for r in store}
    c.judge(f"order store: {len(store)} orders, {sum(1 for k, v in got_final.items() if want_final.get(k) == v)} "
            f"in their final state, expected {len(final)}", got_final == want_final)

    print("pricing: every window that closed, exactly once, and each zone's current price")
    mult = topic("pricing.multipliers")
    got_w = {(r["cell"], r["zone"], r["window_end"]): (r["demand"], r["supply"], r["multiplier_pct"], r["window_start"])
             for r in mult}
    keys = [(r["cell"], r["zone"], r["window_end"]) for r in mult]
    horizon = min(ref.wm_pings, ref.wm_changes)
    beyond = [k for k in got_w if k[2] > horizon]
    judged = [k for k in keys if k[2] <= horizon]
    c.exactly_once("pricing.multipliers topic (windows up to the final watermark)", judged, windows)
    wrong = [k for k in windows if k in got_w and got_w[k] != windows[k]]
    c.judge(f"window values: {len(windows) - len(wrong)} of {len(windows)} as the model says"
            + (f", {len(beyond)} later windows not judged" if beyond else ""), not wrong)
    zp = pg("SELECT zone, cell, multiplier_pct, demand, supply, window_end_ms FROM zone_pricing")
    latest = {}
    for (cell, zone, we), v in got_w.items():
        if zone not in latest or we > latest[zone][0]:
            latest[zone] = (we, v)
    # The table is kept by a second statement of the same job; at the end of
    # the night it can stand a window or two behind the topic, so it is held
    # to the topic's row for the window it shows, within the lag of the latest.
    rows_by = {(r["zone"], r["window_end_ms"]): r for r in zp}
    consistent = all(
        got_w.get((r["cell"], r["zone"], r["window_end_ms"]), (None,))[:3] ==
        (r["demand"], r["supply"], r["multiplier_pct"]) for r in zp)
    behind = {r["zone"]: (latest[r["zone"]][0] - r["window_end_ms"]) // 60_000 for r in zp if r["zone"] in latest}
    c.judge(f"zone_pricing table: {len(zp)} zones, each at a window the topic holds with the same values; "
            f"{sum(1 for b in behind.values() if b == 0)} at the latest window, the rest within "
            f"{max(behind.values()) if behind else 0} windows of it",
            consistent and len(zp) == len(latest) and all(b <= 10 for b in behind.values()))

    print("quotes: every order priced under the window it was placed in, exactly once")
    priced = topic("orders.priced")
    c.exactly_once("orders.priced topic", [r["order_id"] for r in priced], quotes)
    wrong = [r for r in priced if quotes.get(r["order_id"]) != (r["multiplier_pct"], r["decide_at"])]
    c.judge(f"quote values: {len(priced) - len(wrong)} of {len(priced)} as the model says", not wrong)

    print("dispatch: every order assigned to the nearest rider, or escalated, exactly once")
    dec = topic("dispatch.decisions")
    c.exactly_once("dispatch.decisions topic", [r["order_id"] for r in dec], decisions, late_window)
    wrong = [r for r in dec if decisions.get(r["order_id"]) !=
             (r["kind"], r["rider_id"], r["candidates"], r["score"], r["decided_at"])]
    assigned = sum(1 for r in dec if r["kind"] == "assigned")
    c.judge(f"decisions: {assigned} assigned, {len(dec) - assigned} escalated, "
            f"{len(dec) - len(wrong)} of {len(dec)} as the model says", not wrong)

    print("the ledgers: each payout and each credit recorded exactly once")
    pay = pg("SELECT order_id, rider_id, amount_cents, delivered_at_ms, fee_cents, multiplier_pct, "
             "(extract(epoch FROM pg_xact_commit_timestamp(xmin)) * 1000)::bigint AS committed_ms FROM payouts")
    c.exactly_once("payouts table", [r["order_id"] for r in pay], payouts, late_payouts)
    wrong = [r for r in pay if payouts.get(r["order_id"]) !=
             (r["rider_id"], r["amount_cents"], r["delivered_at_ms"], r["fee_cents"], r["multiplier_pct"])]
    total = sum(r["amount_cents"] for r in pay)
    want_total = sum(payouts[r["order_id"]][1] for r in pay if r["order_id"] in payouts)
    c.judge(f"payout values: {len(pay) - len(wrong)} of {len(pay)} as the model says; "
            f"{total} cents paid, the model says {want_total} for those orders",
            not wrong and total == want_total)
    pub = topic("ledger.payouts")
    c.exactly_once("ledger.payouts topic", [r["order_id"] for r in pub], payouts, late_payouts)
    cred = pg("SELECT order_id, reason, amount_cents, at_ms, "
              "(extract(epoch FROM pg_xact_commit_timestamp(xmin)) * 1000)::bigint AS committed_ms FROM credits")
    c.exactly_once("credits table", [(r["order_id"], r["reason"], r["amount_cents"], r["at_ms"]) for r in cred],
                   credits)
    late = sum(1 for r in cred if r["reason"] == "late_delivery")
    print(f"    {late} late deliveries, {len(cred) - late} cancellations, "
          f"{sum(r['amount_cents'] for r in cred)} cents credited")

    print("the analytics store: every row at least once, copies collapsed by key")
    ev = ch("SELECT order_id, status, updated_at_ms FROM halftime.order_events")
    c.at_least_once("order_events", [(r["order_id"], r["status"], r["updated_at_ms"]) for r in ev], expected_changes)
    dd = ch("SELECT order_id, kind, rider_id FROM halftime.dispatch_decisions")
    c.at_least_once("dispatch_decisions", [r["order_id"] for r in dd], decisions, late_window)
    wrong = [r for r in dd if decisions.get(r["order_id"], ())[:2] != (r["kind"], r["rider_id"])]
    c.judge(f"dispatch_decisions values: {len(dd) - len(wrong)} of {len(dd)} as the model says", not wrong)
    zw = ch("SELECT cell, zone, window_end_ms FROM halftime.zone_pricing_windows")
    c.at_least_once("zone_pricing_windows (up to the final watermark)",
                    [(r["cell"], r["zone"], r["window_end_ms"]) for r in zw if r["window_end_ms"] <= horizon], windows)
    rp = ch("SELECT order_id, rider_id, amount_cents FROM halftime.rider_payouts")
    c.at_least_once("rider_payouts", [r["order_id"] for r in rp], payouts, late_payouts)
    wrong = [r for r in rp if payouts.get(r["order_id"], ())[:2] != (r["rider_id"], r["amount_cents"])]
    c.judge(f"rider_payouts values: {len(rp) - len(wrong)} of {len(rp)} as the model says", not wrong)
    earn = ch("SELECT rider_id, order_id, amount_cents, running_cents FROM halftime.rider_earnings")
    totals, by_rider = defaultdict(int), defaultdict(list)
    for r in earn:
        by_rider[r["rider_id"]].append(r)
    consistent = True
    for rider, rows_ in by_rider.items():
        running = 0
        for r in rows_:  # the view orders them
            running += r["amount_cents"]
            consistent = consistent and r["running_cents"] == running
        totals[rider] = running
    want_totals = defaultdict(int)
    for order_id, (rider, amount, _, _, _) in payouts.items():
        if order_id in {r["order_id"] for r in rp}:
            want_totals[rider] += amount
    c.judge(f"rider_earnings view: {len(earn)} rows, running totals consistent, {len(totals)} riders' "
            f"totals as the model says for the payouts recorded", consistent and dict(totals) == dict(want_totals))
    if totals:
        print(f"    the best night: {max(totals.values())} cents")
    spend = ch("SELECT customer_id, order_id, total_cents, spent_cents, delivered_at_ms FROM halftime.customer_spend")
    want_spend = ref.customer_spend()
    tied = {(cust, at) for cust, items in want_spend.items() for at in Counter(i[3] for i in items)
            if Counter(i[3] for i in items)[at] > 1}
    want_rows = {(cust, o, t, sp) for cust, items in want_spend.items() for (o, t, sp, at) in items
                 if (cust, at) not in tied}
    # The stream's partitions end at different times, and the running total
    # waits on the lowest, so the rows past that floor may still be waiting.
    floor = partition_floor(changes, "updated_at")
    tail = {(cust, o, t, sp) for cust, items in want_spend.items() for (o, t, sp, at) in items
            if at > floor}
    got_rows = [(r["customer_id"], r["order_id"], r["total_cents"], r["spent_cents"]) for r in spend
                if r["delivered_at_ms"] <= ref.wm_changes and (r["customer_id"], r["delivered_at_ms"]) not in tied]
    beyond = sum(1 for r in spend if r["delivered_at_ms"] > ref.wm_changes)
    c.at_least_once(f"customer_spend (up to the final watermark; {beyond} rows beyond it and "
                    f"{len(tied)} tied deliveries not judged; {len(tail)} rows past the partitions' "
                    f"floor optional)", got_rows, want_rows, tail)

    print("the archive: every stream as Parquet, exactly once")
    for d, want in (("orders.changes", len(ref.changes)), ("riders.positions", None), ("dispatch.decisions", len(dec))):
        base = ROOT / "state" / "archive" / d
        committed = [f for f in base.rglob("*.parquet") if "staging" not in f.parts] if base.exists() else []
        if not committed:
            c.judge(f"{d}: no committed Parquet files", False)
            continue
        rel = str(committed[0].parent.relative_to(ROOT / "state" / "archive"))
        n = ch(f"SELECT count() AS n FROM file('archive/{rel}/*.parquet', Parquet)")[0]["n"]
        if d == "riders.positions":
            sent = topic_count("riders.positions") if args.scale else len(topic("riders.positions"))
            c.judge(f"{d}: {len(committed)} files, {n} rows, the topic holds {sent}", n == sent)
        elif d == "dispatch.decisions":
            c.judge(f"{d}: {len(committed)} files, {n} rows, the topic holds {want}", n == want)
        else:
            c.judge(f"{d}: {len(committed)} files, {n} rows, expected {want}", n == want)

    print("the city graph")
    graph = subprocess.run([sys.executable, "-I", str(ROOT / "tools" / "load_graph.py"), "--check"],
                           cwd=ROOT, capture_output=True, text=True, env=dict(os.environ))
    print(graph.stdout.rstrip())
    c.ok = c.ok and graph.returncode == 0

    print("how long each path took, wall clock, from a change reaching Kafka (the orders still in")
    print(f"their dispatch window when the quotes stopped, {len(late_window)}, were released by the idle")
    print("timeout and are left out):")
    placed_at = {r["order_id"]: r["appended_ms"] for r in changes if r["op"] == "insert"}
    delivered_at = {r["order_id"]: r["appended_ms"] for r in changes if r["status"] == "delivered"}
    cancelled_at = {r["order_id"]: r["appended_ms"] for r in changes if r["status"] == "cancelled"}
    latency_line("quote published", [r["appended_ms"] - placed_at[r["order_id"]] for r in priced if r["order_id"] in placed_at])
    latency_line("dispatch decision published", [r["appended_ms"] - placed_at[r["order_id"]] for r in dec
                                                 if r["order_id"] in placed_at and r["order_id"] not in late_window])
    latency_line("payout committed", [r["committed_ms"] - delivered_at[r["order_id"]] for r in pay
                                      if r["order_id"] in delivered_at and r["order_id"] not in late_window])
    latency_line("credit committed", [r["committed_ms"] - (delivered_at if r["reason"] == "late_delivery" else cancelled_at)[r["order_id"]]
                                      for r in cred if r["order_id"] in (delivered_at if r["reason"] == "late_delivery" else cancelled_at)])
    print("check_run:", "PASS" if c.ok else "FAIL")
    sys.exit(0 if c.ok else 1)


if __name__ == "__main__":
    main()
