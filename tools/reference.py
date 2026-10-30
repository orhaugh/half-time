#!/usr/bin/env python3
"""What the pipeline must produce for the generated night, computed
independently of clink from state/world alone.

    python3 tools/reference.py            print the expected counts

Each function mirrors one job's definition in pipeline/*.sql, in event time:
pricing windows, quotes, dispatch decisions, payouts and credits. The checks
in tools/check_run.py compare every store with these.
"""
import bisect
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import world as W  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
WORLD = ROOT / "state" / "world"
SENTINEL = 9223372036854775807


def rows(name):
    with open(WORLD / name) as f:
        return [json.loads(line) for line in f if line.strip()]


def window_starts(ts):
    """The starts of the sliding windows a time falls in: five, a minute apart."""
    base = ts // W.HOP_SLIDE_MS * W.HOP_SLIDE_MS
    return [base - j * W.HOP_SLIDE_MS for j in range(W.HOP_SIZE_MS // W.HOP_SLIDE_MS)]


class Reference:
    def __init__(self):
        self.manifest = json.loads((WORLD / "manifest.json").read_text())
        self.cells = json.loads((WORLD / "cells.json").read_text())
        self.restaurants = {r["restaurant_id"]: r for r in rows("restaurants.jsonl")}
        self.orders = rows("orders.jsonl")
        self.changes = rows("changes.jsonl")
        self.pings = rows("pings.jsonl")
        self.kitchen = rows("kitchen.jsonl")
        self.menus = rows("menus.jsonl")
        self.menu_changes = rows("menu_changes.jsonl")
        self.match = rows("match.jsonl")
        self.devices = rows("devices.jsonl")
        # Watermarks at the end of the night: the latest time each stream
        # reached, less the lag every table declares.
        lag = 60_000
        self.wm_pings = max(p["ts"] for p in self.pings) - lag
        self.wm_changes = max(c["updated_at"] for c in self.changes) - lag
        self.wm_kitchen = max(e["ts"] for e in self.kitchen) - W.KITCHEN_LAG_MS
        self._windows = None
        self._quotes = None
        self._decisions = None
        self._payouts = None

    # pipeline/pricing.sql
    def windows(self):
        """(cell, zone, window_end) -> (demand, supply, multiplier_pct, window_start),
        for every window that closes: the union's watermark is the lower of the
        two streams', the riders' reports."""
        if self._windows is None:
            demand, riders = defaultdict(int), defaultdict(set)
            for c in self.changes:
                if c["op"] == "insert":
                    for ws in window_starts(c["updated_at"]):
                        demand[(c["cell"], c["zone"], ws)] += 1
            for p in self.pings:
                for ws in window_starts(p["ts"]):
                    riders[(p["cell"], p["zone"], ws)].add(p["rider_id"])
            out = {}
            for key in set(demand) | set(riders):
                cell, zone, ws = key
                we = ws + W.HOP_SIZE_MS
                if we > min(self.wm_pings, self.wm_changes):
                    continue
                d, s = demand.get(key, 0), len(riders.get(key, ()))
                out[(cell, zone, we)] = (d, s, W.surge_pct(d, s), ws)
            self._windows = out
        return self._windows

    # pipeline/quotes.sql
    def quotes(self):
        """order_id -> (multiplier_pct, decide_at): the window that ended in
        the minute the order was placed, or 1.00x when the zone had none."""
        if self._quotes is None:
            win = self.windows()
            out = {}
            for o in self.orders:
                we = o["placed_at"] // W.MIN * W.MIN
                row = win.get((o["cell"], o["zone"], we))
                out[o["order_id"]] = (row[2] if row else 100, o["placed_at"] + W.DISPATCH_AFTER_MS)
            self._quotes = out
        return self._quotes

    # pipeline/dispatch.sql
    def decisions(self):
        """order_id -> (kind, rider_id, candidates, score, decided_at)."""
        if self._decisions is None:
            by_zone = defaultdict(list)
            for p in self.pings:
                by_zone[p["zone"]].append(p)
            for zone in by_zone:
                by_zone[zone].sort(key=lambda p: p["ts"])
            ts_index = {zone: [p["ts"] for p in ps] for zone, ps in by_zone.items()}
            out = {}
            for o in self.orders:
                r = self.restaurants[o["restaurant_id"]]
                zones = [r["zone"], *self.cells[o["cell"]]["zones"][r["zone"]]]
                lo, hi = o["placed_at"] - W.DISPATCH_BEFORE_MS, o["placed_at"] + W.DISPATCH_AFTER_MS
                best, riders = SENTINEL, set()
                for zone in zones:
                    ps, ts = by_zone.get(zone, []), ts_index.get(zone, [])
                    for i in range(bisect.bisect_left(ts, lo), bisect.bisect_right(ts, hi)):
                        p = ps[i]
                        d2 = (p["x"] - r["x"]) ** 2 + (p["y"] - r["y"]) ** 2
                        if d2 > W.DISPATCH_RADIUS_M ** 2:
                            continue
                        best = min(best, d2 * W.SCORE_SCALE + p["rider_id"])
                        riders.add(p["rider_id"])
                decide_at = o["placed_at"] + W.DISPATCH_AFTER_MS
                decided_at = decide_at // W.MIN * W.MIN + W.MIN
                if riders:
                    out[o["order_id"]] = ("assigned", best % W.SCORE_SCALE, len(riders), best, decided_at)
                else:
                    out[o["order_id"]] = ("escalated", None, 0, SENTINEL, decided_at)
            self._decisions = out
        return self._decisions

    def decided_horizon(self):
        """The dispatcher's watermark from the quotes alone when the night
        ends: the last quote's decide_at, less the lag. A decision window that
        ends after it closes only once the quote stream has gone idle and the
        riders' reports carry the watermark on, so those orders are judged if
        they were decided and reported if they were not."""
        return max(o["placed_at"] for o in self.orders) + W.DISPATCH_AFTER_MS - 60_000

    def late_window(self):
        """The orders still inside their dispatch window at the last quote."""
        horizon = self.decided_horizon()
        return {k for k, v in self.decisions().items() if v[4] > horizon}

    # pipeline/ledgers.sql
    def payouts(self):
        """order_id -> (rider_id, amount_cents, delivered_at_ms, fee_cents, multiplier_pct)."""
        if self._payouts is None:
            quotes, decisions = self.quotes(), self.decisions()
            delivered = {c["order_id"]: c["updated_at"] for c in self.changes if c["status"] == "delivered"}
            out = {}
            for o in self.orders:
                d = decisions[o["order_id"]]
                if o["order_id"] in delivered and d[0] == "assigned":
                    pct = quotes[o["order_id"]][0]
                    out[o["order_id"]] = (d[1], o["fee_cents"] * pct // 100, delivered[o["order_id"]],
                                          o["fee_cents"], pct)
            self._payouts = out
        return self._payouts

    def credits(self):
        """[(order_id, reason, amount_cents, at_ms)]."""
        out = []
        for c in self.changes:
            if c["status"] == "delivered" and c["updated_at"] > c["promised_at"] + W.LATE_GRACE_MS:
                out.append((c["order_id"], "late_delivery", c["total_cents"] * W.LATE_CREDIT_PCT // 100,
                            c["updated_at"]))
            elif c["status"] == "cancelled":
                out.append((c["order_id"], "cancelled", c["total_cents"], c["updated_at"]))
        return out

    def customer_spend(self):
        """customer_id -> [(order_id, total_cents, spent_cents, delivered_at_ms)] in
        delivery order, for the deliveries the order stream's watermark passed;
        the order store commits in that order, so the stream is in order."""
        by_customer = defaultdict(list)
        for c in self.changes:
            if c["status"] == "delivered" and c["updated_at"] <= self.wm_changes:
                by_customer[c["customer_id"]].append((c["updated_at"], c["order_id"], c["total_cents"]))
        out = {}
        for customer, items in by_customer.items():
            items.sort()
            spent, rows_ = 0, []
            for at, order_id, total in items:
                spent += total
                rows_.append((order_id, total, spent, at))
            out[customer] = rows_
        return out

    def earnings(self):
        """rider_id -> [(order_id, amount_cents, running_cents, delivered_at_ms)] in time
        order, for the payouts the published stream's watermark passed."""
        payouts = self.payouts()
        wm = max(p[2] for p in payouts.values()) - 60_000
        by_rider = defaultdict(list)
        for order_id, (rider, amount, at, _, _) in payouts.items():
            if at <= wm:
                by_rider[rider].append((at, order_id, amount))
        out = {}
        for rider, items in by_rider.items():
            items.sort()
            running, rows_ = 0, []
            for at, order_id, amount in items:
                running += amount
                rows_.append((order_id, amount, running, at))
            out[rider] = rows_
        return out

    # pipeline/menus.sql
    def menu_stream(self):
        """(restaurant_id, item_id, updated_at, op) -> (name, price_cents, available):
        the menu store's rows as the night began, then every change."""
        out = {(m["restaurant_id"], m["item_id"], m["updated_at"], "insert"):
               (m["name"], m["price_cents"], m["available"]) for m in self.menus}
        for c in self.menu_changes:
            out[(c["restaurant_id"], c["item_id"], c["updated_at"], "update")] = (c["name"], c["price_cents"], c["available"])
        return out

    def menu_final(self):
        """(restaurant_id, item_id) -> (name, price_cents, available, updated_at) at the end."""
        out = {(m["restaurant_id"], m["item_id"]): (m["name"], m["price_cents"], m["available"], m["updated_at"])
               for m in self.menus}
        for c in self.menu_changes:  # in time order
            out[(c["restaurant_id"], c["item_id"])] = (c["name"], c["price_cents"], c["available"], c["updated_at"])
        return out

    # pipeline/kitchen.sql
    def alerts(self):
        """(restaurant_id, first_at) -> (cell, first_order, last_order, last_at): three
        bounces in a row at a restaurant, in event-time order, each bounce in one
        alert at most."""
        by_restaurant = defaultdict(list)
        for e in self.kitchen:
            by_restaurant[e["restaurant_id"]].append(e)
        out = {}
        for rid, events in by_restaurant.items():
            events.sort(key=lambda e: (e["ts"], e["event_id"]))
            run = []
            for e in events:
                if e["status"] != "bounced":
                    run = []
                    continue
                run.append(e)
                if len(run) == W.ALERT_RUN:
                    out[(rid, run[0]["ts"])] = (run[0]["cell"], run[0]["order_id"], run[-1]["order_id"], run[-1]["ts"])
                    run = []
        return out

    def late_alerts(self):
        """The alerts the tablet stream's watermark had not passed when the stream
        ended; the idle timeout may release them."""
        return {k for k, v in self.alerts().items() if v[3] >= self.wm_kitchen}

    # jobs/riders (the typed C++ job)
    def riders_latest(self):
        """rider_id -> the report the rider app's store keeps: the rider's latest,
        by the phone's clock."""
        out = {}
        for d in self.devices:
            if d["rider_id"] not in out or d["ts"] > out[d["rider_id"]]["ts"]:
                out[d["rider_id"]] = d
        return out

    def low_battery_episodes(self):
        """(rider_id, from_ts) -> until_ts: each stretch from a report below
        LOW_BATTERY_PCT, while the rider was not already in one, to the first report
        back at CHARGED_PCT (None if the night ends first). A command to charge
        belongs to the episode whose stretch holds the report it answers."""
        by_rider = defaultdict(list)
        for d in self.devices:
            by_rider[d["rider_id"]].append(d)
        out = {}
        for rid, reports in by_rider.items():
            reports.sort(key=lambda d: d["ts"])
            start = None
            for d in reports:
                if start is None and d["battery_pct"] < W.LOW_BATTERY_PCT:
                    start = d["ts"]
                    out[(rid, start)] = None
                elif start is not None and d["battery_pct"] >= W.CHARGED_PCT:
                    out[(rid, start)] = d["ts"]
                    start = None
        return out

    # pipeline/match.sql
    def goal_orders(self):
        """(goal_seq, order_id) -> (goal_minute, cell, placed_at): the orders placed
        in the two minutes from each goal."""
        out = {}
        for g in self.match:
            if g["kind"] != "goal":
                continue
            for o in self.orders:
                if g["ts"] <= o["placed_at"] <= g["ts"] + W.GOAL_SURGE_MS:
                    out[(g["seq"], o["order_id"])] = (g["minute"], o["cell"], o["placed_at"])
        return out

    # pipeline/gauges.sql
    def latest_windows(self):
        """zone -> (cell, window_end, multiplier_pct): each zone's latest closed window."""
        out = {}
        for (cell, zone, we), v in self.windows().items():
            if zone not in out or we > out[zone][1]:
                out[zone] = (cell, we, v[2])
        return out


def main():
    ref = Reference()
    d = ref.decisions()
    assigned = sum(1 for v in d.values() if v[0] == "assigned")
    win = ref.windows()
    credits = ref.credits()
    late = sum(1 for c in credits if c[1] == "late_delivery")
    print(f"reference: {len(ref.orders)} orders, {len(ref.changes)} changes, {len(ref.pings)} reports")
    print(f"  pricing windows that close: {len(win)}; multipliers above 1.00x: "
          f"{sum(1 for v in win.values() if v[2] > 100)}")
    print(f"  quotes: {len(ref.quotes())}, above 1.00x: {sum(1 for q in ref.quotes().values() if q[0] > 100)}")
    print(f"  decisions: {assigned} assigned, {len(d) - assigned} escalated, "
          f"{len(ref.late_window())} orders still in their dispatch window at the last quote")
    print(f"  payouts: {len(ref.payouts())}, {sum(p[1] for p in ref.payouts().values())} cents")
    print(f"  credits: {late} late deliveries, {len(credits) - late} cancellations, "
          f"{sum(c[2] for c in credits)} cents")
    bounced = sum(1 for e in ref.kitchen if e["status"] == "bounced")
    goals = sum(1 for e in ref.match if e["kind"] == "goal")
    print(f"  match feed: {len(ref.match)} events, {goals} goals; orders in the two minutes after a goal: "
          f"{len(ref.goal_orders())}")
    print(f"  menu stream: {len(ref.menus)} items, {len(ref.menu_changes)} changes; tablet events: "
          f"{len(ref.kitchen)}, {bounced} bounces; alerts: {len(ref.alerts())}, "
          f"{len(ref.late_alerts())} at the end of the stream")
    print(f"  riders' phones: {len(ref.devices)} reports from {len(ref.riders_latest())} riders; "
          f"low-battery episodes: {len(ref.low_battery_episodes())}")


if __name__ == "__main__":
    main()
